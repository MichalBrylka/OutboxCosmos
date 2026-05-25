using WebProcessing;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<JobQueue>();

builder.Services.AddHostedService<JobWorker>();

var app = builder.Build();

app.MapPost("/jobs", async (string payload, JobQueue queue, CancellationToken cancellationToken) =>
{
    var job = new JobItem(Guid.CreateVersion7(), payload);

    await queue.QueueAsync(job, cancellationToken);

    return Results.Ok(new { job.Id, Message = "Job queued" });
});

app.Run();