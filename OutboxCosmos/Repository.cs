using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace OutboxCosmos;

public interface IOutboxRepository
{
    Task<ICollection<OutboxMessageTargetDocument>> AddMessageWithTargetsAsync(IMessage message, IEnumerable<string> targetNames);
    Task UpdateTargetStatusAsync(OutboxMessageTargetDocument target);
    Task<List<OutboxMessageTargetDocument>> GetPendingTargetsAsync(int maxTotal = 1000, CancellationToken cancellationToken = default);
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
                GetUniqueId(), messageId, message, clock.UtcNowOffset, targetName, OutboxMessageTargetStatus.Pending, 0, null, null
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

    public async Task<List<OutboxMessageTargetDocument>> GetPendingTargetsAsync(int maxTotal = 1000, CancellationToken cancellationToken = default)
    {
        var queryDefinition = new QueryDefinition("""
            SELECT * FROM c WHERE c.status = @status ORDER BY c._ts ASC            
        """)
            .WithParameter("@status", nameof(OutboxMessageTargetStatus.Pending));

        var requestOptions = new QueryRequestOptions { MaxItemCount = 20 }; 

        var iterator = _container.GetItemQueryIterator<OutboxMessageTargetDocument>(queryDefinition, requestOptions: requestOptions);

        var results = new List<OutboxMessageTargetDocument>();

        while (iterator.HasMoreResults && results.Count < maxTotal)
        {
            results.AddRange(await iterator.ReadNextAsync(cancellationToken));
        }

        return results;
    }

    public string GetUniqueId() => Guid.CreateVersion7().ToString();
}