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


builder.Services.AddSingleton<IOutboxRepository, CosmosOutboxRepository>();
builder.Services.AddSingleton<IOutboxMessageHandler, EmailHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, SmsHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, AuditHandler>();

// Channel for in-process dispatching
builder.Services.AddSingleton(_ =>
    Channel.CreateUnbounded<OutboxMessageTarget>(new UnboundedChannelOptions
        { SingleReader = true, SingleWriter = false }));

// --- 5. Background Workers ---
builder.Services.AddHostedService<OutboxDispatcherWorker>(); // Processes Channel
builder.Services.AddHostedService<OutboxRecoveryWorker>(); // Scans DB for "Pending"

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