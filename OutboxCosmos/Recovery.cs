using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxRecoveryWorker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Channel<OutboxMessageTarget> _channel;
    private readonly ILogger<OutboxRecoveryWorker> _logger;

    public OutboxRecoveryWorker(IServiceProvider serviceProvider, Channel<OutboxMessageTarget> channel, ILogger<OutboxRecoveryWorker> logger)
    {
        _serviceProvider = serviceProvider;
        _channel = channel;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // 3. Periodically look for Pending messages
            _logger.LogInformation("Recovery worker scanning for pending messages...");

            using (var scope = _serviceProvider.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
                var pending = await repo.GetPendingTargetsAsync();

                foreach (var target in pending)
                {
                    // Re-enqueue into channel if not already being processed
                    await _channel.Writer.WriteAsync(target, stoppingToken);
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }
}