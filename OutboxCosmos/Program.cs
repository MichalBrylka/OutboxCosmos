using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxCosmos;

var builder = Host.CreateApplicationBuilder(args);

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


builder.Services.AddSingleton<IOutboxChannel, OutboxChannel>();


// --- Background Workers ---
builder.Services.AddHostedService<OutboxDispatcherWorker>();

var app = builder.Build();

await InitializeDatabase(app.Services);

await app.StartAsync();

await AddExampleMessages(app.Services);

await app.StopAsync();


async Task InitializeDatabase(IServiceProvider sp)
{
    var logger = sp.GetRequiredService<ILogger<Program>>();
    var client = sp.GetRequiredService<CosmosClient>();
    var opt = sp.GetRequiredService<IOptions<CosmosOptions>>().Value;

    logger.LogInformation("Ensuring Database/Container exists...");
    var db = await client.CreateDatabaseIfNotExistsAsync(opt.Database);
    await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties(opt.Container, "/messageId"));
}

async Task AddExampleMessages(IServiceProvider sp)
{
    var repo = sp.GetRequiredService<IOutboxRepository>();
    var routingHandler = sp.GetRequiredService<IRoutingHandler>();
    var channel = sp.GetRequiredService<IOutboxChannel>();

    List<IMessage> messages = [
        new TextMessage("Session-123", MessagePriority.High, "Hello via Outbox!"),
        new ImageMessage("Session-456", 1920, 1080, "https://example.com/img.png"),
        new SystemMessage("Session-789", DateTime.UtcNow, "System Heartbeat")
    ];

    foreach (var m in messages)
    {
        var dispatchRequests = await repo.AddMessageWithTargetsAsync(m, routingHandler.GetTargetsForMessage(m));

        foreach (var dr in dispatchRequests)
            await channel.Writer.WriteAsync(dr); //can be skipped for testing recovery 
    }
}