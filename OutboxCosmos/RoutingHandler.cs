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
    public IReadOnlyCollection<string> Visit(RFQRequest message) => [FixGatewayHandler.HandlerName, AuditHandler.HandlerName];

    public IReadOnlyCollection<string> Visit(Quote message) => [FixGatewayHandler.HandlerName];

    public IReadOnlyCollection<string> Visit(QuoteCancel message) => [FixGatewayHandler.HandlerName, AuditHandler.HandlerName];
}