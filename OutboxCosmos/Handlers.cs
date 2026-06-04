using Microsoft.Extensions.Logging;

namespace OutboxCosmos;

public interface IOutboxMessageHandler
{    
    string Name { get; }

    bool SupportRetry { get; }
    Task<Result> Publish(string id, IMessage message);
}

/*public abstract class OutboxMessageHandler(string name, bool supportRetry=false) :    IOutboxMessageHandler,    IAsyncMessageVisitor
{
    public string Name { get; } = name;

    public bool SupportRetry { get; } = supportRetry;
    
    public Task<Result> Publish(        string id,        IMessage message)
    {
        return message.Accept(this);
    }

    public virtual Task<Result> Visit(RFQRequest message)
        => Unsupported(message);

    public virtual Task<Result> Visit(Quote message)
        => Unsupported(message);

    public virtual Task<Result> Visit(QuoteCancel message)
        => Unsupported(message);

    protected Task<Result> Unsupported<TMessage>(TMessage message)
        where TMessage : IMessage
    {
        var result = Result.Fail(
            message:
                $"Handler '{Name}' does not implement publishing for message type '{typeof(TMessage).Name}'.",
            isRetryable: false,
            id: CurrentMessageId);

        return Task.FromResult(result);
    }
}*/

public sealed class FixGatewayHandler : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public const string HandlerName = "FIX-GATEWAY";

    public bool SupportRetry => true; 

    public Task<Result> Publish(string id, IMessage message)
    {
        Console.WriteLine($"[FIX-GATEWAY] Sending {id}: {message}");
        return Task.FromResult(Result.Ok());
    }
}

public sealed class AuditHandler : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public const string HandlerName = "AUDIT";

    public bool SupportRetry => false; 

    public Task<Result> Publish(string id, IMessage message)
    {
        Console.WriteLine($"[AUDIT] Logging {id}: {message}");
        return Task.FromResult(Result.Ok());
    }
}

public class NullHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public const string HandlerName = "null";

    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message)
    {
        logger.LogWarning("[NULL] Message {Id} will not be sent to any handler: {Message}", id, message);

        return Task.FromResult(Result.Ok());
    }
}