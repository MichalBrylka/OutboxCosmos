using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OutboxCosmos;

public class RecoveryService(IOutboxRepository repository, IOutboxChannel channel, ILogger<RecoveryService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;

        logger.LogInformation("Recovery scanning for pending messages...");
        List<OutboxDispatchRequest> pending = await repository.GetPendingTargetIdsAsync(cancellationToken: cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Recovery will be performed for {MessagesNumber}: {DocumentIds}", pending.Count, string.Join(", ", pending.Select(p => p.DocumentId)));

        foreach (var p in pending)
            await channel.Writer.WriteAsync(p, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
