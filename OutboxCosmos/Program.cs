using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using OutboxCosmos;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

var builder = Host.CreateApplicationBuilder(args);

// --- Strongly Typed Configuration ---
builder
    .RegisterOptions<CosmosOptions>()
    .RegisterOptions<RetryOptions>()
    .RegisterOptions<MessageRoutingOptions>()
    ;



// --- Cosmos DB Client & Serializer ---
builder.Services.AddSingleton(sp =>
{
    var options = sp.GetRequiredService<IOptions<CosmosOptions>>().Value;
    var jsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter(null, true) }
    };
    return new CosmosClient(options.Endpoint, options.Key, new CosmosClientOptions
    {
        Serializer = new SystemTextJsonCosmosSerializer(jsonOptions),
        ConnectionMode = ConnectionMode.Direct
    });
});

// --- 4. Services & Repositories ---
builder.Services.AddSingleton<IOutboxRepository, CosmosOutboxRepository>();
builder.Services.AddSingleton<IOutboxMessageHandler, EmailHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, SmsHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, AuditHandler>();

// Channel for in-process dispatching
builder.Services.AddSingleton(_ => Channel.CreateUnbounded<OutboxMessageTarget>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false }));

// --- 5. Background Workers ---
builder.Services.AddHostedService<OutboxDispatcherWorker>(); // Processes Channel
builder.Services.AddHostedService<OutboxRecoveryWorker>();   // Scans DB for "Pending"

var host = builder.Build();

// --- 8. Run Demo ---
await InitializeDatabase(host.Services);
await RunDemo(host.Services);

await host.StartAsync();

Console.WriteLine("""
    
    --- Outbox System Running ---
    Press 'R' to replay failed/dead messages. Press 'Q' to quit.
    -----------------------------

    """);


while (true)
{
    if (!Console.KeyAvailable)
    {
        await Task.Delay(100); // Prevent CPU spiking
        continue;
    }

    var key = Console.ReadKey(true).Key;

    if (key == ConsoleKey.R)
    {
        Console.WriteLine("\n[Manual Trigger] Replaying failed messages...");
        using var scope = host.Services.CreateScope();
        var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        // This flips DeadLettered -> Pending, which the RecoveryWorker will pick up
        var count = await repo.ReplayFailedMessagesAsync();
        Console.WriteLine($">>> {count} targets reset to Pending for recovery scan.\n");
    }
    else if (key == ConsoleKey.Q)
    {
        Console.WriteLine("\nShutting down...");
        break;
    }
}

// 4. Graceful Shutdown
await host.StopAsync();








// ==========================================
// CORE LOGIC & REPOSITORY
// ==========================================

async Task InitializeDatabase(IServiceProvider sp)
{
    var logger = sp.GetRequiredService<ILogger<Program>>();
    var client = sp.GetRequiredService<CosmosClient>();
    var opt = sp.GetRequiredService<IOptions<CosmosOptions>>().Value;

    logger.LogInformation("Ensuring Database/Container exists...");
    var db = await client.CreateDatabaseIfNotExistsAsync(opt.Database);
    await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties(opt.Container, "/messageId"));
}

async Task RunDemo(IServiceProvider sp)
{
    var repo = sp.GetRequiredService<IOutboxRepository>();
    var routing = sp.GetRequiredService<IOptions<MessageRoutingOptions>>().Value;
    var channel = sp.GetRequiredService<Channel<OutboxMessageTarget>>();

    var messages = new List<IMessage>
    {
        new TextMessage("Session-123", MessagePriority.High, "Hello via Outbox!"),
        new ImageMessage("Session-456", 1920, 1080, "https://example.com/img.png"),
        new SystemMessage("Session-789", DateTime.UtcNow, "System Heartbeat")
    };

    foreach (var msg in messages)
    {
        var id = Guid.CreateVersion7().ToString();
        var outboxMsg = new OutboxMessage(id, msg, DateTimeOffset.UtcNow);

        // Determine targets based on config
        var typeName = msg.GetType().Name;
        if (routing.TryGetValue(typeName, out var targetNames))
        {
            var targets = targetNames.Select(t => new OutboxMessageTarget(
                Guid.CreateVersion7().ToString(), id, t, OutboxMessageTargetStatus.Pending, 0, null, null, null, null
            )).ToList();

            // 4. Transactional Batch Save
            await repo.AddMessageWithTargetsAsync(outboxMsg, targets);

            // 3. Immediate Dispatch via Channel
            //can be skipped for testing recovery 
            foreach (var t in targets) await channel.Writer.WriteAsync(t);
        }
    }
}

// --- Repository Implementation ---
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

// ==========================================
// BACKGROUND WORKERS (The "Engine")
// ==========================================

public class OutboxDispatcherWorker : BackgroundService
{
    private readonly Channel<OutboxMessageTarget> _channel;
    private readonly IServiceProvider _serviceProvider;
    private readonly AsyncRetryPolicy _retryPolicy;
    private readonly ILogger<OutboxDispatcherWorker> _logger;
    private readonly RetryOptions _retryOptions;

    public OutboxDispatcherWorker(Channel<OutboxMessageTarget> channel, IServiceProvider serviceProvider, IOptions<RetryOptions> retryOptions, ILogger<OutboxDispatcherWorker> logger)
    {
        _channel = channel;
        _serviceProvider = serviceProvider;
        _logger = logger;
        _retryOptions = retryOptions.Value;


        // 3. Resiliency with Polly
        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(_retryOptions.MaxAttempts,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time, retryCount, context) =>
                    _logger.LogWarning($"Retry {retryCount} due to: {ex.Message}"));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Dispatcher Started. Monitoring channel...");

        // ReadAllAsync keeps the loop alive until the channel is closed
        await foreach (var target in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            using var scope = _serviceProvider.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var handlers = scope.ServiceProvider.GetServices<IOutboxMessageHandler>();

            var handler = handlers.FirstOrDefault(h => h.Name == target.TargetName);

            try
            {
                // 1. Fetch the actual message payload
                var message = await repo.GetMessageAsync(target.MessageId);
                if (message == null || handler == null)
                {
                    _logger.LogWarning($"Skipping target {target.Id}: Message or Handler not found.");
                    continue;
                }

                // 2. Execute with Polly Resiliency
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation($"Attempting to publish {target.TargetName} for message {target.MessageId}...");

                    await handler.Publish(message);

                    // 3. Success: Update status to Dispatched
                    var successTarget = target with
                    {
                        Status = OutboxMessageTargetStatus.Dispatched,
                        DispatchedAtUtc = DateTimeOffset.UtcNow,
                        LastError = null // Clear any previous errors
                    };
                    await repo.UpdateTargetStatusAsync(successTarget);
                    _logger.LogInformation($"Successfully dispatched {target.Id}: {target.MessageId}");
                });
            }
            catch (Exception ex)
            {
                // 4. Dead Letter: If Polly retries are exhausted, it throws here
                _logger.LogError($"Permanent failure for target {target.Id} after retries. Moving to DeadLetter. Error: {ex.Message}");

                var deadTarget = target with
                {
                    Status = OutboxMessageTargetStatus.DeadLettered,
                    LastError = ex.Message,
                    RetryCount = _retryOptions.MaxAttempts // Marking that we hit the ceiling
                };

                try
                {
                    await repo.UpdateTargetStatusAsync(deadTarget);
                }
                catch (Exception dbEx)
                {
                    _logger.LogCritical($"Failed to even update DeadLetter status in DB: {dbEx.Message}");
                }
            }
        }
    }
}

public class OutboxRecoveryWorker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Channel<OutboxMessageTarget> _channel;
    private readonly ILogger<OutboxRecoveryWorker> _logger;

    public OutboxRecoveryWorker(IServiceProvider serviceProvider, Channel<OutboxMessageTarget> channel, ILogger<OutboxRecoveryWorker> logger)
    {
        _serviceProvider = serviceProvider;
        _channel = channel;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // 3. Periodically look for Pending messages
            _logger.LogInformation("Recovery worker scanning for pending messages...");

            using (var scope = _serviceProvider.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
                var pending = await repo.GetPendingTargetsAsync();

                foreach (var target in pending)
                {
                    // Re-enqueue into channel if not already being processed
                    await _channel.Writer.WriteAsync(target, stoppingToken);
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }
}