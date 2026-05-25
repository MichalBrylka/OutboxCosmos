using System.Threading.Channels;

namespace WebProcessing;

public record JobItem(Guid Id, string Payload);

public class JobQueue
{
    private readonly Channel<JobItem> _channel;

    public JobQueue()
    {
        _channel = Channel.CreateBounded<JobItem>(
            new BoundedChannelOptions(100)
            {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
    }

    public ValueTask QueueAsync(JobItem job, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(job, cancellationToken);

    public IAsyncEnumerable<JobItem> ReadAllAsync(CancellationToken cancellationToken = default)
        => _channel.Reader.ReadAllAsync(cancellationToken);

    public void Complete() => _channel.Writer.Complete();
}

public class JobWorker(JobQueue queue, ILogger<JobWorker> logger) : BackgroundService
{
    private readonly JobQueue _queue = queue;
    private readonly ILogger<JobWorker> _logger = logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Start 4 parallel workers
        var workers = Enumerable.Range(1, 4).Select(workerId => RunWorkerAsync(workerId, stoppingToken)).ToArray();

        await Task.WhenAll(workers);
    }

    private async Task RunWorkerAsync(int workerId, CancellationToken cancellationToken)
    {
        await foreach (var job in _queue.ReadAllAsync(cancellationToken))
        {
            try
            {
                _logger.LogInformation("Worker {WorkerId} START job {JobId}", workerId, job.Id);

                // Simulate work
                await Task.Delay(Random.Shared.Next(2000, 7000), cancellationToken);

                _logger.LogInformation("Worker {WorkerId} END job {JobId}", workerId, job.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Worker {WorkerId} failed processing job {JobId}", workerId, job.Id);
            }
        }
    }
}