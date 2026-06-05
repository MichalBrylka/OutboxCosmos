using Microsoft.Extensions.Logging;

namespace OutboxCosmos;

public interface IOutboxMessageHandler
{
    string Name { get; }

    bool SupportRetry { get; }
    Task<Result> Publish(string id, IMessage message, CancellationToken cancellationToken = default);
}

public sealed record PublishContext(string Id, CancellationToken CancellationToken = default);

public abstract class OutboxMessageHandlerBase(string name, bool supportRetry = false) : IOutboxMessageHandler, IMessageVisitor<PublishContext, Task<Result>>
{
    public string Name { get; } = name;

    public bool SupportRetry { get; } = supportRetry;

    public async Task<Result> Publish(string id, IMessage message, CancellationToken cancellationToken = default)
    {
        try
        {
            return await message.Accept(new PublishContext(id, cancellationToken), this);
        }
        catch (Exception ex)
        {
            return Result.Fail(
                message: $"Unhandled exception at '{Name}' ",
                isRetryable: SupportRetry, exception: ex, id: id
                );
        }
    }

    public virtual Task<Result> Visit(PublishContext context, RFQRequest message)
        => Unsupported(context, message);

    public virtual Task<Result> Visit(PublishContext context, Quote message)
        => Unsupported(context, message);

    public virtual Task<Result> Visit(PublishContext context, QuoteCancel message)
        => Unsupported(context, message);

    protected Result Ok<TMessage>(PublishContext context) where TMessage : IMessage
        => Result.Ok($"Message {context.Id} was sent successfully");

    private Task<Result> Unsupported<TMessage>(PublishContext context, TMessage message) where TMessage : IMessage
        => Task.FromResult(
            Result.Fail(
            message: $"Handler '{Name}' does not implement publishing for message type '{typeof(TMessage).Name}': {message}",
            isRetryable: false, id: context.Id
            )
        );

    private Result Failed<TMessage>(PublishContext context, TMessage message, Exception exception) where TMessage : IMessage
    {
        //TODO
    }
}

public sealed class FixGatewayHandler : OutboxMessageHandlerBase
{
    public const string HandlerName = "FIX-GATEWAY";

    public FixGatewayHandler() : base(HandlerName, supportRetry: true) { }

    public override async Task<Result> Visit(PublishContext context, RFQRequest message)
    {
        try
        {
            await SendToFixSession(context, message);
            return Result.Ok($"RFQ sent: {message.RFQId}");
        }
        catch (Exception ex)
        {
            return Result.Fail(
                message: $"Failed to send RFQ {message.RFQId}",
                isRetryable: SupportRetry,
                exception: ex,
                id: context.Id);
        }
    }

    public override async Task<Result> Visit(PublishContext context, Quote message)
    {
        try
        {
            await SendToFixSession(context, message);
            return Result.Ok($"Quote sent: {message.QuoteId}");
        }
        catch (Exception ex)
        {
            return Result.Fail(
                message: $"Failed to send Quote {message.QuoteId}",
                isRetryable: SupportRetry,
                exception: ex,
                id: context.Id);
        }
    }

    public override async Task<Result> Visit(PublishContext context, QuoteCancel message)
    {
        try
        {
            await SendToFixSession(context, message);
            return Result.Ok($"Cancel sent: {message.QuoteId}");
        }
        catch (Exception ex)
        {
            return Result.Fail(
                message: $"Failed to send Cancel {message.QuoteId}",
                isRetryable: SupportRetry,
                exception: ex,
                id: context.Id);
        }
    }

    private static Task SendToFixSession(PublishContext ctx, IMessage msg)
    {
        Console.WriteLine($"[FIX] {ctx.Id} -> {msg}");

        if (Random.Shared.NextDouble() < 0.5)
            throw new InvalidOperationException($"Simulated FIX session failure for message {ctx.Id}");

        return Task.CompletedTask;
    }
}

public sealed class AuditHandler : OutboxMessageHandlerBase
{
    public const string HandlerName = "AUDIT";

    public AuditHandler() : base(name: HandlerName, supportRetry: false) { }

    public override Task<Result> Visit(PublishContext context, RFQRequest message)
    {
        Log("RFQ", message.RFQId, context.Id);
        return Task.FromResult(Result.Ok("RFQ audited"));
    }

    public override Task<Result> Visit(PublishContext context, Quote message)
    {
        Log("QUOTE", message.QuoteId, context.Id);
        return Task.FromResult(Result.Ok("Quote audited"));
    }

    public override Task<Result> Visit(PublishContext context, QuoteCancel message)
    {
        Log("CANCEL", message.QuoteId, context.Id);
        return Task.FromResult(Result.Ok("Cancel audited"));
    }

    private static void Log(string type, string id, string ctxId) => Console.WriteLine($"[AUDIT] ctx={ctxId} type={type} id={id}");
}


public class NullHandler(ILogger<IOutboxMessageHandler> logger) : IOutboxMessageHandler
{
    public string Name => HandlerName;
    public const string HandlerName = "null";

    public bool SupportRetry => false;

    public Task<Result> Publish(string id, IMessage message, CancellationToken cancellationToken = default)
    {
        logger.LogError("[NULL] Message {Id} will not be sent to any handler: {Message}", id, message);
        return Task.FromResult(Result.Ok($"Message {id} handling finished"));
    }
}