namespace OutboxCosmos;

public interface IOutboxMessageHandler
{
    string Name { get; }
    Task Publish(OutboxMessage message);
}

public class EmailHandler : IOutboxMessageHandler
{
    public string Name => "email";

    public Task Publish(OutboxMessage message)
    {
        // 100% chance of failure
        if (Random.Shared.NextDouble() < 1.0)
            throw new Exception("SMTP Server timed out (Simulated).");

        Console.WriteLine($"[EMAIL] Sent message {message.Id}: {message.Payload}");
        return Task.CompletedTask;
    }
}

public class SmsHandler : IOutboxMessageHandler
{
    public string Name => "sms";

    public Task Publish(OutboxMessage message)
    {
        Console.WriteLine($"[SMS] Sent message {message.Id}: {message.Payload}");
        return Task.CompletedTask;
    }
}

public class AuditHandler : IOutboxMessageHandler
{
    public string Name => "audit";

    public Task Publish(OutboxMessage message)
    {
        Console.WriteLine($"[AUDIT] Logged message {message.Id}: {message.Payload}");
        return Task.CompletedTask;
    }
}