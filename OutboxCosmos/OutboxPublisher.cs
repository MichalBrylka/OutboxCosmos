namespace OutboxCosmos;

public interface IOutboxPublisher
{
    Task PublishAsync(IMessage message);
}

public class OutboxPublisher(IOutboxRepository repository, IRoutingHandler routingHandler, IOutboxChannel channel) : IOutboxPublisher
{
    public async Task PublishAsync(IMessage message)
    {
        var targets = routingHandler.GetTargetsForMessage(message);
        var dispatchRequests = await repository.AddAsync(message, targets);
        
        foreach (var dr in dispatchRequests) await channel.Writer.WriteAsync(dr); //can be skipped for testing recovery         
    }
}
