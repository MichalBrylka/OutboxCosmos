using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OutboxCosmos;

public interface IOptionsConfiguration
{
    static virtual string? ConfigurationSectionName { get; } = null;
}

public class CosmosOptions : IOptionsConfiguration
{
    public static string ConfigurationSectionName => "Cosmos";

    public string Endpoint { get; set; } = default!;
    public string Key { get; set; } = default!;
    public string Database { get; set; } = default!;
    public string Container { get; set; } = default!;
}

public class RetryOptions : IOptionsConfiguration
{
    public static string ConfigurationSectionName => "Retry";

    public int MaxAttempts { get; set; }
}

public class MessageRoutingOptions : Dictionary<string, List<string>>, IOptionsConfiguration
{
    public static string ConfigurationSectionName => "MessageRouting";
}

public static class OptionsExtensions
{
    public static IHostApplicationBuilder RegisterOptions<TOptions>(this IHostApplicationBuilder builder)
        where TOptions : class, IOptionsConfiguration
    {
        builder.Services.Configure<TOptions>(
            builder.Configuration.GetSection(TOptions.ConfigurationSectionName ?? typeof(TOptions).Name)
            );
        return builder;
    }
}