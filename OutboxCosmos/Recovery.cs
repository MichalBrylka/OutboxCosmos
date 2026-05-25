using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace OutboxCosmos;

public interface IRecoveryService
{
    Task RunRecoveryAsync(CancellationToken cancellationToken = default);
}

public class RecoveryService(IOutboxRepository repository, Channel<OutboxMessageTargetDocument> channel, ILogger<RecoveryService> logger) : IRecoveryService
{
    public async Task RunRecoveryAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return;

        logger.LogInformation("Recovery worker scanning for pending messages...");
        var pending = await repository.GetPendingTargetsAsync(cancellationToken: cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Recovery will be performed for {MessagesNumber}: {IDs}", pending.Count, string.Join(", ", pending.Select(p => p.Id)));

        foreach (var target in pending)
            await channel.Writer.WriteAsync(target, cancellationToken); // Re-enqueue into channel if not already being processed
    }
}