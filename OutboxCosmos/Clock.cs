namespace OutboxCosmos;

public interface IClock
{
    DateTimeOffset UtcNowOffset { get; }

    DateTime UtcNow() => UtcNowOffset.UtcDateTime;
}

public sealed class SystemClock : IClock
{
    public DateTimeOffset UtcNowOffset => DateTimeOffset.UtcNow;
}