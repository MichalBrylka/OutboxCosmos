using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace OutboxCosmos;

public interface IOutboxRepository
{
    Task<List<OutboxDispatchRequest>> AddMessageWithTargetsAsync(IMessage message, IReadOnlyCollection<string> targetNames, CancellationToken cancellationToken = default);
    Task<OutboxMessageTargetDocument?> GetAsync(string id, string messageId, CancellationToken cancellationToken = default);

    Task MarkAsDispatchedAsync(OutboxDispatchRequest request, DateTimeOffset dispatchedAtUtc, int retryCount, CancellationToken cancellationToken = default);
    Task MarkAsDeadLetterAsync(OutboxDispatchRequest request, string lastError, int retryCount, CancellationToken cancellationToken = default);


    Task<List<OutboxDispatchRequest>> GetPendingTargetIdsAsync(int maxTotal = 1000, CancellationToken cancellationToken = default);
    string GetUniqueId();
}

public class CosmosOutboxRepository(CosmosClient client, IOptions<CosmosOptions> options, IClock clock) : IOutboxRepository
{
    private readonly Container _container = client.GetContainer(options.Value.Database, options.Value.Container);

    public async Task<List<OutboxDispatchRequest>> AddMessageWithTargetsAsync(IMessage message, IReadOnlyCollection<string> targetNames, CancellationToken cancellationToken = default)
    {
        if (targetNames == null || targetNames.Count == 0) return [];

        var messageId = GetUniqueId();

        var batch = _container.CreateTransactionalBatch(new PartitionKey(messageId));

        var result = new List<OutboxDispatchRequest>(targetNames.Count);

        foreach (var targetName in targetNames)
        {
            var documentId = GetUniqueId();

            var document = new OutboxMessageTargetDocument(
                Id: documentId,
                MessageId: messageId,
                Payload: message,
                CreatedAt: clock.UtcNowOffset,
                TargetName: targetName,
                Status: OutboxMessageTargetStatus.Pending,
                RetryCount: 0,
                DispatchedAtUtc: null,
                LastError: null
            );

            batch.CreateItem(document);

            result.Add(new OutboxDispatchRequest(DocumentId: documentId, MessageId: messageId, TargetName: targetName));
        }

        using var response = await batch.ExecuteAsync(cancellationToken);

        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"Outbox batch insert failed: {response.ErrorMessage}");

        return result;
    }

    public async Task<OutboxMessageTargetDocument?> GetAsync(string id, string messageId, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _container.ReadItemAsync<OutboxMessageTargetDocument>(id, new PartitionKey(messageId), cancellationToken: cancellationToken);
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task MarkAsDispatchedAsync(OutboxDispatchRequest request, DateTimeOffset dispatchedAtUtc, int retryCount, CancellationToken cancellationToken = default)
    {
        await _container.PatchItemAsync<OutboxMessageTargetDocument>(
            request.DocumentId,
            new PartitionKey(request.MessageId),
            [
                PatchOperation.Set("/status", nameof(OutboxMessageTargetStatus.Dispatched)),
                PatchOperation.Set("/dispatchedAtUtc", dispatchedAtUtc),
                PatchOperation.Set("/retryCount", retryCount),
                PatchOperation.Set("/lastError", "")
            ],
            cancellationToken: cancellationToken);
    }

    public async Task MarkAsDeadLetterAsync(OutboxDispatchRequest request, string lastError, int retryCount, CancellationToken cancellationToken = default)
    {
        await _container.PatchItemAsync<OutboxMessageTargetDocument>(
            request.DocumentId,
            new PartitionKey(request.MessageId),
            [
                PatchOperation.Set("/status", nameof(OutboxMessageTargetStatus.DeadLettered)),
                PatchOperation.Set("/lastError", lastError),
                PatchOperation.Set("/retryCount", retryCount)
            ],
            cancellationToken: cancellationToken);
    }

    public async Task<List<OutboxDispatchRequest>> GetPendingTargetIdsAsync(int maxTotal = 1000, CancellationToken cancellationToken = default)
    {
        var query = new QueryDefinition("""
        SELECT
            c.id AS DocumentId,
            c.messageId AS MessageId,
            c.targetName AS TargetName
        FROM c
        WHERE c.status = @status
        ORDER BY c._ts ASC
    """)
        .WithParameter("@status", nameof(OutboxMessageTargetStatus.Pending));

        var iterator = _container.GetItemQueryIterator<OutboxDispatchRequest>(query, requestOptions: new QueryRequestOptions { MaxItemCount = 100 });

        var results = new List<OutboxDispatchRequest>(Math.Min(maxTotal, 1024));

        while (iterator.HasMoreResults && results.Count < maxTotal)
        {
            results.AddRange(await iterator.ReadNextAsync(cancellationToken));
        }

        return results;

    }

    public string GetUniqueId() => Guid.CreateVersion7().ToString();
}