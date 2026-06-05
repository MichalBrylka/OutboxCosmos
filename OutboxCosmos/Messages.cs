namespace OutboxCosmos;

public interface IMessage
{
    void Accept(IMessageVisitor visitor);
    T Accept<T>(IMessageVisitor<T> visitor);

    TResult Accept<TContext, TResult>(TContext context, IMessageVisitor<TContext, TResult> visitor);
}

public sealed record RFQRequest(string RFQId, string Symbol, decimal Quantity) : IMessage
{
    public void Accept(IMessageVisitor visitor) => visitor.Visit(this);
    public T Accept<T>(IMessageVisitor<T> visitor) => visitor.Visit(this);

    public TResult Accept<TContext, TResult>(TContext context, IMessageVisitor<TContext, TResult> visitor) 
        => visitor.Visit(context, this);
}

public sealed record Quote(string QuoteId, string RFQId, decimal Price) : IMessage
{
    public void Accept(IMessageVisitor visitor) => visitor.Visit(this);
    public T Accept<T>(IMessageVisitor<T> visitor) => visitor.Visit(this);
    public TResult Accept<TContext, TResult>(TContext context, IMessageVisitor<TContext, TResult> visitor) 
        => visitor.Visit(context, this);
}

public sealed record QuoteCancel(string QuoteId, string Reason) : IMessage
{
    public void Accept(IMessageVisitor visitor) => visitor.Visit(this);
    public T Accept<T>(IMessageVisitor<T> visitor) => visitor.Visit(this);
    public TResult Accept<TContext, TResult>(TContext context, IMessageVisitor<TContext, TResult> visitor) 
        => visitor.Visit(context, this);
}


public record OutboxMessageTargetDocument(
    string Id,

    string MessageId,
    IMessage Payload,
    DateTimeOffset CreatedAt,
    string TargetName, //should match IOutboxMessageHandler.Name

    OutboxMessageTargetStatus Status,
    int RetryCount,
    DateTimeOffset? DispatchedAtUtc = null,
    string? LastError = null
    );
public enum OutboxMessageTargetStatus { Pending, Dispatched, DeadLettered }


public sealed record OutboxDispatchRequest(string DocumentId, string MessageId, string TargetName);