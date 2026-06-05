namespace OutboxCosmos;

public interface IRoutingHandler
{
    IOutboxMessageHandler GetHandlerForTarget(string targetName);
    IReadOnlyCollection<string> GetTargetsForMessage(IMessage message);
}

public sealed class RoutingHandler(IEnumerable<IOutboxMessageHandler> handlers, IRoutingVisitor routingVisitor) : IRoutingHandler
{
    private readonly Dictionary<string, IOutboxMessageHandler> _handlers = handlers.ToDictionary(h => h.Name, h => h);

    public IOutboxMessageHandler GetHandlerForTarget(string targetName) =>
        _handlers.TryGetValue(targetName, out var handler)
            ? handler
            : throw new KeyNotFoundException($"No handler registered for target '{targetName}'");

    public IReadOnlyCollection<string> GetTargetsForMessage(IMessage message) => message.Accept(routingVisitor);
}

public sealed class RoutingVisitor : IRoutingVisitor
{
    public const string FIX = FixGatewayHandler.HandlerName;
    public const string AUDIT = AuditHandler.HandlerName;

    public IReadOnlyCollection<string> Visit(RFQRequest message) => [FIX, AUDIT];

    public IReadOnlyCollection<string> Visit(Quote message) => message.Price > 1_000_000M ? [FIX, AUDIT] : [FIX];

    public IReadOnlyCollection<string> Visit(QuoteCancel message) => [FIX, AUDIT];
}