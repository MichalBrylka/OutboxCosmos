using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace OutboxCosmos;

public class OutboxDispatcherWorker(IOutboxRepository repository, IRoutingHandler routingHandler, IOutboxChannel channel,
    IOptions<OutboxOptions> outboxOptions, ILogger<OutboxDispatcherWorker> logger) : BackgroundService
{
    private const string RETRY_COUNT_KEY = "RetryCount";

    private readonly ILogger<OutboxDispatcherWorker> _logger = logger;
    private readonly OutboxOptions _outboxOptions = outboxOptions.Value;


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
                        logger.LogWarning("Retry {RetryCount} due to retryable failure: ({ID}){Message}, {Exception}", retryCount, failure.Id, failure.ErrorMessage, failure.Exception);
                });

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await RecoverAsync(stoppingToken);

        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");

        var consumers = Enumerable.Range(0, _outboxOptions.ConsumerCount)
            .Select(_ => ConsumeAsync(stoppingToken))
            .ToArray();

        await Task.WhenAll(consumers);
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        await foreach (OutboxDispatchRequest dispatchRequest in channel.Reader.ReadAllAsync(stoppingToken))
        {
            var targetDocument = await repository.GetAsync(dispatchRequest.DocumentId, dispatchRequest.MessageId, stoppingToken);

            if (targetDocument == null) { _logger.LogWarning("Skipping target {OutboxDispatchRequest}: Message is empty.", dispatchRequest); continue; }

            var handler = routingHandler.GetHandlerForTarget(targetDocument.TargetName);

            try
            {
                Result result;
                int? retryCount = null;

                _logger.LogInformation("Attempting to publish {TargetName} for message {DocumentId}...", targetDocument.TargetName, targetDocument.Id);

                if (handler.SupportRetry)
                {
                    var context = new Context();
                    result = await _retryPolicy.ExecuteAsync(async ctx => await handler.Publish(targetDocument.Id, targetDocument.Payload), context);
                    retryCount = context.TryGetValue(RETRY_COUNT_KEY, out var value) ? (int)value : 0;
                }
                else
                {
                    result = await handler.Publish(targetDocument.Id, targetDocument.Payload);
                }

                if (result is Success)
                {
                    await repository.MarkAsDispatchedAsync(dispatchRequest, retryCount ?? 0, stoppingToken);
                    _logger.LogInformation("Successfully dispatched {DocumentId}: {MessageId} for {TargetName}", targetDocument.Id, targetDocument.MessageId, targetDocument.TargetName);
                }
                else
                {
                    var failure = (Failure)result;

                    _logger.LogError("Failed to dispatch target {DocumentId}. Retryable: {Retryable}. Error: {Error}", targetDocument.Id, failure.IsRetryable, failure.ErrorMessage);

                    await repository.MarkAsDeadLetterAsync(dispatchRequest, failure.ErrorMessage, retryCount ?? 0, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Permanent failure for target {DocumentId} after retries. Moving to DeadLetter.", targetDocument.Id);

                try
                {
                    await repository.MarkAsDeadLetterAsync(dispatchRequest, ex.Message, -1, stoppingToken);
                }
                catch (Exception dbEx)
                { _logger.LogCritical(dbEx, "Failed to update DeadLetter status in DB."); }
            }
        }
    }

    private int _recoveryRun = 0; // ensure RunRecovery runs only once
    private async Task RecoverAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _recoveryRun, 1) != 0)
            return;

        if (cancellationToken.IsCancellationRequested)
            return;

        try
        {
            _logger.LogInformation("Recovery scanning for pending messages...");
            var pending = await repository.GetPendingTargetIdsAsync(_outboxOptions.RecoveryBatchSize, cancellationToken);

            if (pending.Count == 0)
                return;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Recovery will be performed for {MessagesNumber}: {DocumentIds}", pending.Count, string.Join(", ", pending.Select(p => p.DocumentId)));

            foreach (var p in pending)
                await channel.Writer.WriteAsync(p, cancellationToken);
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Failed during recovery process. No pending messages will be dispatched until next restart.");
            throw; //prevent host from starting in bad state 
        }
    }
}