using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace OutboxCosmos;

public class OutboxRecoveryWorker(IOutboxRepository repository, Channel<OutboxMessageTargetDocument> channel, ILogger<OutboxRecoveryWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // 3. Periodically look for Pending messages
            logger.LogInformation("Recovery worker scanning for pending messages...");

            var pending = await repository.GetPendingTargetsAsync();

            foreach (var target in pending)
                await channel.Writer.WriteAsync(target, stoppingToken); // Re-enqueue into channel if not already being processed

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }
}