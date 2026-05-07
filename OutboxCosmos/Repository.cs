using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using System.Net;

namespace OutboxCosmos;

public interface IOutboxRepository
{
    Task AddMessageWithTargetsAsync(OutboxMessage message, IEnumerable<OutboxMessageTarget> targets);
    Task<OutboxMessage?> GetMessageAsync(string messageId);
    Task UpdateTargetStatusAsync(OutboxMessageTarget target);
    Task<List<OutboxMessageTarget>> GetPendingTargetsAsync(int limit = 50);
    Task<int> ReplayFailedMessagesAsync();
}

public class CosmosOutboxRepository : IOutboxRepository
{
    private readonly Container _container;
    public CosmosOutboxRepository(CosmosClient client, IOptions<CosmosOptions> options)
    {
        _container = client.GetContainer(options.Value.Database, options.Value.Container);
    }

    public async Task AddMessageWithTargetsAsync(OutboxMessage message, IEnumerable<OutboxMessageTarget> targets)
    {
        // All items share 'messageId' as partition key for transactional consistency
        var batch = _container.CreateTransactionalBatch(new PartitionKey(message.Id));

        // We use a custom 'type' property internally or just store them. 
        // To distinguish in Cosmos, we'll ensure the POCOs serialize with a discriminator if needed, 
        // but here they are unique enough by schema.
        batch.CreateItem(message);
        foreach (var target in targets) batch.CreateItem(target);

        using var response = await batch.ExecuteAsync();
        if (!response.IsSuccessStatusCode) throw new Exception($"Batch failed: {response.ErrorMessage}");
    }

    public async Task<OutboxMessage?> GetMessageAsync(string messageId)
    {
        try
        {
            var response = await _container.ReadItemAsync<OutboxMessage>(messageId, new PartitionKey(messageId));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { return null; }
    }

    public async Task UpdateTargetStatusAsync(OutboxMessageTarget target)
    {
        await _container.UpsertItemAsync(target, new PartitionKey(target.MessageId));
    }

    public async Task<List<OutboxMessageTarget>> GetPendingTargetsAsync(int limit = 50)
    {
        var queryDefinition = new QueryDefinition("""            
            SELECT TOP @limit * FROM c WHERE IS_DEFINED(c.targetName) AND c.status = @status ORDER BY c._ts ASC
            """)
            .WithParameter("@limit", limit)
            .WithParameter("@status", nameof(OutboxMessageTargetStatus.Pending));

        var iterator = _container.GetItemQueryIterator<OutboxMessageTarget>(queryDefinition);
        var results = new List<OutboxMessageTarget>();

        while (iterator.HasMoreResults)
        {
            results.AddRange(await iterator.ReadNextAsync());
        }
        return results;
    }

    public async Task<int> ReplayFailedMessagesAsync()
    {
        var queryDefinition = new QueryDefinition("""            
            SELECT * FROM c WHERE IS_DEFINED(c.targetName) AND c.status IN (@status1, @status2)
            """)
            .WithParameter("@status1", nameof(OutboxMessageTargetStatus.DeadLettered))
            .WithParameter("@status2", nameof(OutboxMessageTargetStatus.ReplyRequested))
            ;

        var iterator = _container.GetItemQueryIterator<OutboxMessageTarget>(queryDefinition);
        int count = 0;

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            foreach (var target in response)
            {
                // Reset to Pending to let the workers pick it up again
                var updated = target with
                {
                    Status = OutboxMessageTargetStatus.Pending,
                    RetryCount = 0,
                    LastError = "Replay triggered manually"
                };
                await UpdateTargetStatusAsync(updated);
                count++;
            }
        }
        return count;
    }
}