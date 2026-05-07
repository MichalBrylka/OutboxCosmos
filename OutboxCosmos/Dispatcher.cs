using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxDispatcherWorker : BackgroundService
{
    private readonly Channel<OutboxMessageTarget> _channel;
    private readonly IServiceProvider _serviceProvider;
    private readonly AsyncRetryPolicy _retryPolicy;
    private readonly ILogger<OutboxDispatcherWorker> _logger;
    private readonly RetryOptions _retryOptions;

    public OutboxDispatcherWorker(Channel<OutboxMessageTarget> channel, IServiceProvider serviceProvider, IOptions<RetryOptions> retryOptions, ILogger<OutboxDispatcherWorker> logger)
    {
        _channel = channel;
        _serviceProvider = serviceProvider;
        _logger = logger;
        _retryOptions = retryOptions.Value;


        // 3. Resiliency with Polly
        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(_retryOptions.MaxAttempts,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time, retryCount, context) =>
                    _logger.LogWarning($"Retry {retryCount} due to: {ex.Message}"));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");

        // ReadAllAsync keeps the loop alive until the channel is closed
        await foreach (var target in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            using var scope = _serviceProvider.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var handlers = scope.ServiceProvider.GetServices<IOutboxMessageHandler>();

            var handler = handlers.FirstOrDefault(h => h.Name == target.TargetName);

            try
            {
                // 1. Fetch the actual message payload
                var message = await repo.GetMessageAsync(target.MessageId);
                if (message == null || handler == null)
                {
                    _logger.LogWarning($"Skipping target {target.Id}: Message or Handler not found.");
                    continue;
                }

                // 2. Execute with Polly Resiliency
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation($"Attempting to publish {target.TargetName} for message {target.MessageId}...");

                    await handler.Publish(message);

                    // 3. Success: Update status to Dispatched
                    var successTarget = target with
                    {
                        Status = OutboxMessageTargetStatus.Dispatched,
                        DispatchedAtUtc = DateTimeOffset.UtcNow,
                        LastError = null // Clear any previous errors
                    };
                    await repo.UpdateTargetStatusAsync(successTarget);
                    _logger.LogInformation($"Successfully dispatched {target.Id}: {target.MessageId}");
                });
            }
            catch (Exception ex)
            {
                // 4. Dead Letter: If Polly retries are exhausted, it throws here
                _logger.LogError($"Permanent failure for target {target.Id} after retries. Moving to DeadLetter. Error: {ex.Message}");

                var deadTarget = target with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = ex.Message,
                    RetryCount = _retryOptions.MaxAttempts // Marking that we hit the ceiling
                };

                try
                {
                    await repo.UpdateTargetStatusAsync(deadTarget);
                }
                catch (Exception dbEx)
                {
                    _logger.LogCritical($"Failed to even update DeadLetter status in DB: {dbEx.Message}");
                }
            }
        }
    }
}