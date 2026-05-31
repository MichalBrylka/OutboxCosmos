namespace OutboxCosmos;


public interface IMessage { }

public enum MessagePriority { Low, Normal, High }

public abstract record BaseMessage(string SourceSession) : IMessage;

public record TextMessage(string SourceSession, MessagePriority Priority = MessagePriority.Low, string Text = "") : BaseMessage(SourceSession);
public record ImageMessage(string SourceSession, int Width, int Height, string Url = "") : BaseMessage(SourceSession);
public record SystemMessage(string SourceSession, DateTime Timestamp, string Content = "") : BaseMessage(SourceSession);


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