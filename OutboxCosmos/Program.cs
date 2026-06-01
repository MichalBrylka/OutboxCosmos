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

builder.Services.AddSingleton<IOutboxPublisher, OutboxPublisher>();


// --- Background Workers ---
builder.Services.AddHostedService<OutboxDispatcherWorker>();

var app = builder.Build();

await InitializeDatabase(app.Services);

await app.StartAsync();

var publisher = app.Services.GetRequiredService<IOutboxPublisher>();
List<IMessage> messages = [
            new TextMessage("Session-123", MessagePriority.High, "Hello via Outbox!"),
            new ImageMessage("Session-456", 1920, 1080, "https://example.com/img.png"),
            new SystemMessage("Session-789", DateTime.UtcNow, "System Heartbeat")
         ];
foreach (var message in messages) await publisher.PublishAsync(message);


Console.WriteLine("Application started.");
Console.WriteLine("Press 'M' to send a manual SystemMessage. Press Escape to exit.");

var listeningTask = Task.Run(async () =>
{
    while (true)
    {
        var keyInfo = Console.ReadKey(intercept: true);
        if (keyInfo.Key == ConsoleKey.M)
        {
            var manualMessage = new SystemMessage(Guid.CreateVersion7().ToString(), DateTime.UtcNow, "Manual System Message");
            try
            {
                await publisher.PublishAsync(manualMessage);
                Console.WriteLine($"[Info] Manual SystemMessage published (MessageId: {manualMessage.SourceSession}).");
            }
            catch (Exception ex) { Console.Error.WriteLine($"[Error] Failed to publish manual message: {ex.Message}"); }
        }
        else if (keyInfo.Key == ConsoleKey.Escape)
        {
            Console.WriteLine("Escape pressed. Exiting...");
            break;
        }
    }
});

// wait for the listening loop to finish (Escape pressed)
await listeningTask;


await app.StopAsync();

static async Task InitializeDatabase(IServiceProvider sp)
{
    var logger = sp.GetRequiredService<ILogger<Program>>();
    var client = sp.GetRequiredService<CosmosClient>();
    var opt = sp.GetRequiredService<IOptions<CosmosOptions>>().Value;

    logger.LogInformation("Ensuring Database/Container exists...");
    var db = await client.CreateDatabaseIfNotExistsAsync(opt.Database);
    await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties(opt.Container, "/messageId"));
}
