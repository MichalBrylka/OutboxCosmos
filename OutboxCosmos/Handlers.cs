using Microsoft.Extensions.Logging;

namespace OutboxCosmos;

public interface IOutboxMessageHandler
{
    static virtual string HandlerName { get; } = "";
    string Name { get; }

    bool SupportRetry { get; }
    Task<Result> Publish(string id, IMessage message);
}

public class EmailHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "email";

    public bool SupportRetry => true;

    public Task<Result> Publish(string id, IMessage message)
    {
        if (Random.Shared.NextDouble() < 0.5)
            return Task.FromResult(
                Result.Fail("SMTP Server timed out (Simulated).", isRetryable: true, id: id)
            );

        logger.LogInformation("[EMAIL] Sent message {Id}: {Message}", id, message);

        return Task.FromResult(Result.Ok("Email sent"));
    }
}

public class SmsHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "sms";
    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        logger.LogInformation("[SMS] Sent message {Id}: {Message}", id, message);

        return Task.FromResult(Result.Ok("SMS sent"));
    }
}

public class AuditHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "audit";
    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        logger.LogInformation("[AUDIT] Logged message {Id}: {Message}", id, message);

        return Task.FromResult(Result.Ok());
    }
}

public class NullHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "null";

    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        logger.LogWarning("[NULL] Message {Id} will not be sent to any handler: {Message}", id, message);

        return Task.FromResult(Result.Ok());
    }
}