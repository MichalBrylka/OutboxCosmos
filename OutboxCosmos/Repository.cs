using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace OutboxCosmos;

public interface IOutboxRepository
{
    Task<ICollection<OutboxMessageTargetDocument>> AddMessageWithTargetsAsync(IMessage message, IEnumerable<string> targetNames);
    Task UpdateTargetStatusAsync(OutboxMessageTargetDocument target);
    Task<List<OutboxMessageTargetDocument>> GetPendingTargetsAsync(int limit = 50);
    Task<int> ReplayFailedMessagesAsync();
    string GetUniqueId();
}

public class CosmosOutboxRepository(CosmosClient client, IOptions<CosmosOptions> options, IClock clock) : IOutboxRepository
{
    private readonly Container _container = client.GetContainer(options.Value.Database, options.Value.Container);

    public async Task<ICollection<OutboxMessageTargetDocument>> AddMessageWithTargetsAsync(IMessage message, IEnumerable<string> targetNames)
    {
        if (targetNames == null || !targetNames.Any()) return [];

        var result = new List<OutboxMessageTargetDocument>();

        var messageId = GetUniqueId();

        var batch = _container.CreateTransactionalBatch(new PartitionKey(messageId));

        foreach (var targetName in targetNames)
        {
            var targetDocument = new OutboxMessageTargetDocument(
                GetUniqueId(), messageId, message, clock.UtcNowOffset, targetName, OutboxMessageTargetStatus.Pending, 0, null, null, null, null
            );
            result.Add(targetDocument);
            batch.CreateItem(targetDocument);
        }

        using var response = await batch.ExecuteAsync();
        if (!response.IsSuccessStatusCode) throw new Exception($"Batch failed: {response.ErrorMessage}");

        return result;
    }

    public async Task UpdateTargetStatusAsync(OutboxMessageTargetDocument target)
    {
        await _container.UpsertItemAsync(target, new PartitionKey(target.MessageId));
    }

    public async Task<List<OutboxMessageTargetDocument>> GetPendingTargetsAsync(int limit = 50)
    {
        var queryDefinition = new QueryDefinition("""            
            SELECT TOP @limit * FROM c WHERE c.status = @status ORDER BY c._ts ASC
            """)
            .WithParameter("@limit", limit)
            .WithParameter("@status", nameof(OutboxMessageTargetStatus.Pending));

        var iterator = _container.GetItemQueryIterator<OutboxMessageTargetDocument>(queryDefinition);
        var results = new List<OutboxMessageTargetDocument>();

        while (iterator.HasMoreResults)
        {
            results.AddRange(await iterator.ReadNextAsync());
        }
        return results;
    }

    public async Task<int> ReplayFailedMessagesAsync()
    {
        var queryDefinition = new QueryDefinition("""            
            SELECT * FROM c WHERE c.status IN (@status1, @status2)
            """)
            .WithParameter("@status1", nameof(OutboxMessageTargetStatus.DeadLettered))
            .WithParameter("@status2", nameof(OutboxMessageTargetStatus.ReplyRequested))
            ;

        var iterator = _container.GetItemQueryIterator<OutboxMessageTargetDocument>(queryDefinition);
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

    public string GetUniqueId() => Guid.CreateVersion7().ToString();
}