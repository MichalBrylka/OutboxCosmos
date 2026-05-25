using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxCosmos;
using Polly;
using Polly.Registry;
using Polly.Retry;
using System.Threading.Channels;

var builder = Host.CreateApplicationBuilder(args);

// --- Strongly Typed Configuration ---
builder
    .RegisterOptions<CosmosOptions>()
    .RegisterOptions<OutboxOptions>()
    ;

builder.Services.AddSingleton(sp =>
{
    var options = sp.GetRequiredService<IOptions<CosmosOptions>>().Value;
    var jsonOptions = sp.GetRequiredService<IJsonOptionsFactory>().Create();

    return new CosmosClient(options.Endpoint, options.Key, new CosmosClientOptions
    {
        Serializer = new SystemTextJsonCosmosSerializer(jsonOptions),
        ConnectionMode = ConnectionMode.Direct
    });
});

builder.Services.AddSingleton<IClock, SystemClock>();
builder.Services.AddSingleton<IRoutingHandler, RoutingHandler>();

builder.Services.AddSingleton<IMessageJsonPolymorphicRegistration, UniversalJsonPolymorphicRegistration>();
builder.Services.AddSingleton<IJsonOptionsFactory, JsonOptionsFactory>();

builder.Services.AddSingleton<IOutboxRepository, CosmosOutboxRepository>();
builder.Services.AddSingleton<IOutboxMessageHandler, EmailHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, SmsHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, AuditHandler>();
builder.Services.AddSingleton<IOutboxMessageHandler, NullHandler>();

// Channel for in-process dispatching
builder.Services.AddSingleton(_ =>
    Channel.CreateUnbounded<OutboxMessageTargetDocument>(new UnboundedChannelOptions
    { SingleReader = true, SingleWriter = false }));

// --- Background Workers ---
builder.Services.AddHostedService<OutboxDispatcherWorker>(); // Processes Channel
//builder.Services.AddHostedService<OutboxRecoveryWorker>(); // Scans DB for "Pending"

var host = builder.Build();

// --- Run Demo ---
await InitializeDatabase(host.Services);
await RunDemo(host.Services);

await host.StartAsync();

Console.WriteLine("""

                  --- Outbox System Running ---
                  Press 'Q' to quit.
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

    if (key == ConsoleKey.Q)
    {
        Console.WriteLine("\nShutting down...");
        break;
    }
}

// Graceful Shutdown
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
    var routingHandler = sp.GetRequiredService<IRoutingHandler>();
    var channel = sp.GetRequiredService<Channel<OutboxMessageTargetDocument>>();

    List<IMessage> messages = [
        new TextMessage("Session-123", MessagePriority.High, "Hello via Outbox!"),
        new ImageMessage("Session-456", 1920, 1080, "https://example.com/img.png"),
        new SystemMessage("Session-789", DateTime.UtcNow, "System Heartbeat")
    ];

    foreach (var m in messages)
    {
        var targetDocuments = await repo.AddMessageWithTargetsAsync(m, routingHandler.GetTargetsForMessage(m));

        // Immediate Dispatch via Channel
        foreach (var t in targetDocuments)
            await channel.Writer.WriteAsync(t); //can be skipped for testing recovery 

    }
}