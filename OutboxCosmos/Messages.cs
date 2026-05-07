namespace OutboxCosmos;

using System.Text.Json.Serialization;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(TextMessage), typeDiscriminator: "text")]
[JsonDerivedType(typeof(ImageMessage), typeDiscriminator: "image")]
[JsonDerivedType(typeof(SystemMessage), typeDiscriminator: "system")]
public interface IMessage { }

public enum MessagePriority { Low, Normal, High }

public abstract record BaseMessage(string SourceSession) : IMessage;

public record TextMessage(string SourceSession, MessagePriority Priority = MessagePriority.Low, string Text = "") : BaseMessage(SourceSession);
public record ImageMessage(string SourceSession, int Width, int Height, string Url = "") : BaseMessage(SourceSession);
public record SystemMessage(string SourceSession, DateTime Timestamp, string Content = "") : BaseMessage(SourceSession);


public record OutboxMessage(string Id, IMessage Payload, DateTimeOffset CreatedAt)
{
    public string MessageId { get; init; } = Id;
}

public record OutboxMessageTarget(
    string Id,
    string MessageId,
    string TargetName, //should match IOutboxMessageHandler.Name
    OutboxMessageTargetStatus Status,
    int RetryCount,
    DateTimeOffset? DispatchedAtUtc,
    string? LastError,
    DateTimeOffset? ReplyRequestedAtUtc,
    string? ReplyRequestedBy
    );
public enum OutboxMessageTargetStatus { Pending, Dispatched, DeadLettered, ReplyRequested }



