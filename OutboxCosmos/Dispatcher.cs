using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxDispatcherWorker : BackgroundService
{
    private readonly IOutboxRepository _repository;
    private readonly IEnumerable<IOutboxMessageHandler> _handlers;
    private readonly IClock _clock;
    private readonly Channel<OutboxMessageTargetDocument> _channel;
    private readonly AsyncRetryPolicy _retryPolicy;
    private readonly ILogger<OutboxDispatcherWorker> _logger;
    private readonly RetryOptions _retryOptions;

    public OutboxDispatcherWorker(IOutboxRepository repository, IEnumerable<IOutboxMessageHandler> handlers, IClock clock, Channel<OutboxMessageTargetDocument> channel, IOptions<RetryOptions> retryOptions, ILogger<OutboxDispatcherWorker> logger)
    {
        _repository = repository;
        _handlers = handlers;
        _clock = clock;
        _channel = channel;
        _retryOptions = retryOptions.Value;
        _logger = logger;

        // Resiliency with Polly
        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(_retryOptions.MaxAttempts,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, _, retryCount, _) =>
                    _logger.LogWarning("Retry {RetryCount} due to: {Message}", retryCount, ex.Message));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");

        // ReadAllAsync keeps the loop alive until the channel is closed
        await foreach (var targetDocument in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            var handler = _handlers.FirstOrDefault(h => h.Name == targetDocument.TargetName);

            try
            {
                if (targetDocument.Payload == null)
                {
                    _logger.LogWarning("Skipping target {TargetId}: Message is empty.", targetDocument.Id);
                    continue;
                }

                // 2. Execute with Polly Resiliency
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation("Attempting to publish {TargetName} for message {MessageId}...", targetDocument.TargetName, targetDocument.MessageId);

                    await handler.Publish(targetDocument.MessageId, targetDocument.Payload);

                    var successTarget = targetDocument with
                    {
                        Status = OutboxMessageTargetStatus.Dispatched,
                        DispatchedAtUtc = _clock.UtcNowOffset,
                        LastError = null // Clear any previous errors
                    };
                    await _repository.UpdateTargetStatusAsync(successTarget);
                    _logger.LogInformation("Successfully dispatched {TargetId}: {MessageId}", targetDocument.Id, targetDocument.MessageId);
                });
            }
            catch (Exception ex)
            {
                // 4. Dead Letter: If Polly retries are exhausted, it throws here
                _logger.LogError("Permanent failure for target {TargetId} after retries. Moving to DeadLetter. Error: {ExMessage}", targetDocument.Id, ex.Message);

                var deadTarget = targetDocument with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = ex.Message,
                    RetryCount = _retryOptions.MaxAttempts // Marking that we hit the ceiling
                };

                try
                {
                    await _repository.UpdateTargetStatusAsync(deadTarget);
                }
                catch (Exception dbEx)
                {
                    _logger.LogCritical("Failed to even update DeadLetter status in DB: {DbExMessage}", dbEx.Message);
                }
            }
        }
    }
}