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
        try
        {
            if (Random.Shared.NextDouble() < 0.5)
                return Task.FromResult(
                    Result.Fail("SMTP Server timed out (Simulated).", isRetryable: true)
                );

            logger.LogInformation("[EMAIL] Sent message {Id}: {Message}", id, message);

            return Task.FromResult(Result.Ok("Email sent"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(Result.Fail("Unexpected email failure", isRetryable: true, ex));
        }
    }
}

public class SmsHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "sms";
    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        try
        {
            logger.LogInformation("[SMS] Sent message {Id}: {Message}", id, message);

            return Task.FromResult(Result.Ok("SMS sent"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(Result.Fail("SMS send failed", isRetryable: false, ex));
        }
    }
}

public class AuditHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{    
    public string Name => HandlerName;
    public static string HandlerName => "audit";
    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        try
        {
            logger.LogInformation("[AUDIT] Logged message {Id}: {Message}", id, message);

            return Task.FromResult(Result.Ok());
        }
        catch (Exception ex)
        {
            return Task.FromResult(Result.Fail("Audit logging failed", isRetryable: false, ex));
        }
    }
}

public class NullHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public static string HandlerName => "null";
        
    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        logger.LogInformation("[NULL] Message {Id} will not be sent to any handler: {Message}", id, message);

        return Task.FromResult(Result.Ok());
    }
}