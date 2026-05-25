using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxDispatcherWorker(IOutboxRepository repository, IRoutingHandler routingHandler, IClock clock, Channel<OutboxMessageTargetDocument> channel,
    IOptions<OutboxOptions> outboxOptions, IPolicyRegistry<string> policyRegistry, ILogger<OutboxDispatcherWorker> logger) : BackgroundService
{
    private readonly IAsyncPolicy _retryPolicy = policyRegistry.Get<IAsyncPolicy>("OutboxPolicy");
    private readonly ILogger<OutboxDispatcherWorker> _logger = logger;
    private readonly OutboxOptions _outboxOptions = outboxOptions.Value;


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");
                
        await foreach (var targetDocument in channel.Reader.ReadAllAsync(stoppingToken))
        {
            var handler = routingHandler.GetHandlerForTarget(targetDocument.TargetName);

            try
            {
                if (targetDocument.Payload == null)
                {
                    _logger.LogWarning("Skipping target {TargetId}: Message is empty.", targetDocument.Id);
                    continue;
                }
                                


                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation("Attempting to publish {TargetName} for message {MessageId}...", targetDocument.TargetName, targetDocument.MessageId);

                    await handler.Publish(targetDocument.MessageId, targetDocument.Payload);

                    
                    var successTarget = targetDocument with
                    {
                        Status = OutboxMessageTargetStatus.Dispatched,
                        DispatchedAtUtc = clock.UtcNowOffset,
                        LastError = null // Clear any previous errors
                    };
                    await repository.UpdateTargetStatusAsync(successTarget);
                    _logger.LogInformation("Successfully dispatched {TargetId}: {MessageId}", targetDocument.Id, targetDocument.MessageId);
                });
            }
            catch (Exception ex)
            {
                // Dead Letter: If Polly retries are exhausted, it throws here
                _logger.LogError("Permanent failure for target {TargetId} after retries. Moving to DeadLetter. Error: {ExMessage}", targetDocument.Id, ex.Message);

                var deadTarget = targetDocument with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = ex.Message,
                    RetryCount = _outboxOptions.MaxRetryAttempts // Marking that we hit the ceiling
                };

                try
                {
                    await repository.UpdateTargetStatusAsync(deadTarget);
                }
                catch (Exception dbEx)
                {
                    _logger.LogCritical("Failed to even update DeadLetter status in DB: {DbExMessage}", dbEx.Message);
                }
            }
        }
    }
}