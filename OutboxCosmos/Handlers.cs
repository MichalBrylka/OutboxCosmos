using Microsoft.Extensions.Logging;

namespace OutboxCosmos;

public interface IOutboxMessageHandler
{
    string Name { get; }
    Task Publish(string id, IMessage message);
}

public class EmailHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => "email";

    public Task Publish(string id, IMessage message)
    {
        // 100% chance of failure
        if (Random.Shared.NextDouble() < 1.0)
            throw new Exception("SMTP Server timed out (Simulated).");

        logger.LogInformation("[EMAIL] Sent message {Id}: {Message}", id, message);
        return Task.CompletedTask;
    }
}

public class SmsHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => "sms";

    public Task Publish(string id, IMessage message)
    {
        logger.LogInformation("[SMS] Sent message {Id}: {Message}", id, message);
        return Task.CompletedTask;
    }
}

public class AuditHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => "audit";

    public Task Publish(string id, IMessage message)
    {
        logger.LogInformation("[AUDIT] Logged message {Id}: {Message}", id, message);
        return Task.CompletedTask;
    }
}