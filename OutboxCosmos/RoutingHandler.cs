using Microsoft.Extensions.Options;

namespace OutboxCosmos;

public class RoutingHandler(IOptions<MessageRoutingOptions> routingOptionsOption, IEnumerable<IOutboxMessageHandler> handlers)
{    
    private readonly IReadOnlyDictionary<string, List<string>> typeNameToTargets = routingOptionsOption.Value;
    private readonly IReadOnlyDictionary<string, IOutboxMessageHandler> targetToHandlers = handlers.ToDictionary(h => h.Name, h => h);
    private readonly IReadOnlyCollection<string> possibleDestinations = handlers.Select(h => h.Name).Where(d => !string.IsNullOrWhiteSpace(d)).ToHashSet(StringComparer.Ordinal);


    public IReadOnlyCollection<string> GetTargetsForMessage(IMessage message)
    {
        var typeName = message.GetType().Name;
        if (typeNameToTargets.TryGetValue(typeName, out var targetNames))
            return targetNames;
        return possibleDestinations; //design decision - if no explicit mapping is defined, send to all possible destinations
    }

    public IOutboxMessageHandler GetHandlerForTarget(string targetName) =>
        targetToHandlers.TryGetValue(targetName, out var handler)
            ? handler
            : throw new Exception($"No handler found for target '{targetName}'. Check your configuration and ensure a handler with this name is registered.");
}