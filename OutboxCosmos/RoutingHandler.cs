namespace OutboxCosmos;

public interface IRoutingHandler
{
    IOutboxMessageHandler GetHandlerForTarget(string targetName);
    IReadOnlyCollection<string> GetTargetsForMessage(IMessage message);
}

public class RoutingHandler(IEnumerable<IOutboxMessageHandler> handlers) : IRoutingHandler
{
    private readonly IReadOnlyDictionary<string, IOutboxMessageHandler> targetToHandlers = handlers.ToDictionary(h => h.Name, h => h);


    public IReadOnlyCollection<string> GetTargetsForMessage(IMessage message) => message switch
    {
        TextMessage tm => GetTargetsForTextMessage(tm),
        ImageMessage im => GetTargetsForImageMessage(im),
        SystemMessage sm => GetTargetsForSystemMessage(sm),
        _ => [NullHandler.HandlerName]
    };

    private static List<string> GetTargetsForTextMessage(TextMessage message)
    {
        List<string> targets = [AuditHandler.HandlerName];

        if (message.Priority == MessagePriority.High)
            targets.AddRange([EmailHandler.HandlerName, SmsHandler.HandlerName]);
        else if (message.Priority == MessagePriority.Normal)
            targets.Add(EmailHandler.HandlerName);

        return targets;
    }

    private static List<string> GetTargetsForImageMessage(ImageMessage message)
    {
        List<string> targets = [AuditHandler.HandlerName];

        var size = message.Width * message.Height;

        if (size > 1_000_000)
            targets.Add(EmailHandler.HandlerName);
        else
            targets.AddRange([EmailHandler.HandlerName, SmsHandler.HandlerName]);

        return targets;
    }

    private static List<string> GetTargetsForSystemMessage(SystemMessage message)
    {
        List<string> targets = [AuditHandler.HandlerName];

        if (message.Content.Contains("error", StringComparison.OrdinalIgnoreCase))
            targets.Add(EmailHandler.HandlerName);

        return targets;
    }

    public IOutboxMessageHandler GetHandlerForTarget(string targetName) =>
        targetToHandlers.TryGetValue(targetName, out var handler)
            ? handler
            : throw new Exception($"No handler found for target '{targetName}'. Check your configuration and ensure a handler with this name is registered."); //fail-fast approach to catch misconfigurations early
}