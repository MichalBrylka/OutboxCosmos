using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxDispatcherWorker(IOutboxRepository repository, IRoutingHandler routingHandler, IClock clock, Channel<OutboxMessageTargetDocument> channel,
    IOptions<OutboxOptions> outboxOptions, ILogger<OutboxDispatcherWorker> logger) : BackgroundService
{
    private const string RETRY_COUNT_KEY = "RetryCount";

    private readonly ILogger<OutboxDispatcherWorker> _logger = logger;
    private readonly AsyncRetryPolicy<Result> _retryPolicy = Policy<Result>
            .Handle<Exception>()
            .OrResult(result => result is Failure failure && failure.IsRetryable)
            .WaitAndRetryAsync(
                outboxOptions.Value.MaxRetryAttempts,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 200)),//exponential backoff with jitter
                onRetry: (outcome, delay, retryCount, context) =>
                {
                    context[RETRY_COUNT_KEY] = retryCount;

                    if (outcome.Exception is not null)
                        logger.LogWarning(outcome.Exception, "Retry {RetryCount} due to exception: {Message}", retryCount, outcome.Exception.Message);
                    else if (outcome.Result is Failure failure)
                        logger.LogWarning("Retry {RetryCount} due to retryable failure: {Message}, {Exception}", retryCount, failure.ErrorMessage, failure.Exception);
                });

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");

        await foreach (var targetDocument in channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (targetDocument.Payload == null)
            {
                _logger.LogWarning("Skipping target {TargetId}: Message is empty.", targetDocument.Id);
                continue;
            }

            var handler = routingHandler.GetHandlerForTarget(targetDocument.TargetName);

            try
            {
                if (targetDocument.Payload == null)
                {
                    _logger.LogWarning(
                        "Skipping target {TargetId}: Message is empty.",
                        targetDocument.Id);

                    continue;
                }

                Result result;
                int? retryCount = null;

                _logger.LogInformation("Attempting to publish {TargetName} for message {MessageId}...", targetDocument.TargetName, targetDocument.MessageId);

                if (handler.SupportRetry)
                {
                    var context = new Context();
                    result = await _retryPolicy.ExecuteAsync(async ctx => await handler.Publish(targetDocument.MessageId, targetDocument.Payload), context);
                    retryCount = context.TryGetValue(RETRY_COUNT_KEY, out var value) ? (int)value : 0;
                }
                else
                {
                    result = await handler.Publish(targetDocument.MessageId, targetDocument.Payload);
                }

                if (result is Success)
                {
                    var successTarget = targetDocument with
                    {
                        Status = OutboxMessageTargetStatus.Dispatched,
                        DispatchedAtUtc = clock.UtcNowOffset,
                        LastError = null,
                        RetryCount = retryCount ?? 0
                    };
                    await repository.UpdateTargetStatusAsync(successTarget);

                    _logger.LogInformation("Successfully dispatched {TargetId}: {MessageId}", targetDocument.Id, targetDocument.MessageId);

                    continue;
                }

                var failure = (Failure)result;

                _logger.LogError("Failed to dispatch target {TargetId}. Retryable: {Retryable}. Error: {Error}", targetDocument.Id, failure.IsRetryable, failure.ErrorMessage);

                var deadTarget = targetDocument with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = failure.ErrorMessage,
                    RetryCount = retryCount ?? 0
                };

                await repository.UpdateTargetStatusAsync(deadTarget);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Permanent failure for target {TargetId} after retries. Moving to DeadLetter.", targetDocument.Id);

                var deadTarget = targetDocument with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = ex.Message,
                    RetryCount = -1
                };

                try
                { await repository.UpdateTargetStatusAsync(deadTarget); }
                catch (Exception dbEx)
                { _logger.LogCritical(dbEx, "Failed to update DeadLetter status in DB."); }
            }
        }
    }
}