using System.Threading.Channels;

var channel = Channel.CreateBounded<Job>(
    new BoundedChannelOptions(100)
    {
        SingleReader = false,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.Wait
    });

// Start 4 workers
var workers = Enumerable.Range(1, 4)
    .Select(workerId => WorkerAsync(workerId, channel.Reader))
    .ToArray();

// Start multiple producers
var producers = Enumerable.Range(1, 3)
    .Select(producerId => ProducerAsync(producerId, channel.Writer))
    .ToArray();

// Wait for producers
await Task.WhenAll(producers);

// Signal no more messages
channel.Writer.Complete();

// Wait for workers to finish
await Task.WhenAll(workers);

Console.WriteLine("All done.");


// --------------------------------------------------

static async Task ProducerAsync(int producerId, ChannelWriter<Job> writer)
{
    for (int i = 1; i <= 10; i++)
    {
        var job = new Job(Id: Guid.CreateVersion7(), Name: $"P{producerId}-Job{i}");

        await writer.WriteAsync(job);

        Console.WriteLine($"Produced {job.Name}");

        await Task.Delay(Random.Shared.Next(100, 300));
    }
}

static async Task WorkerAsync(int workerId, ChannelReader<Job> reader)
{
    await foreach (var job in reader.ReadAllAsync())
    {
        Console.WriteLine(            $"Worker {workerId} START {job.Name}");

        // Simulate variable work duration
        await Task.Delay(Random.Shared.Next(1000, 5000));

        Console.WriteLine(
            $"Worker {workerId} END   {job.Name}");
    }
}

record Job(Guid Id, string Name);