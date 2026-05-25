using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace OutboxCosmos;

public interface IOptionsConfiguration
{
    static virtual string? ConfigurationSectionName { get; } = null;
}

public class CosmosOptions : IOptionsConfiguration
{
    public static string ConfigurationSectionName => "Cosmos";

    [Required]
    public string Endpoint { get; set; } = default!;
    public string Key { get; set; } = default!;
    public string Database { get; set; } = default!;
    public string Container { get; set; } = default!;
}

public class OutboxOptions : IOptionsConfiguration
{
    public static string ConfigurationSectionName => "Outbox";

    public int MaxRetryAttempts { get; set; }
}

public static class OptionsExtensions
{
    public static IHostApplicationBuilder RegisterOptions<TOptions>(
          this IHostApplicationBuilder builder)
          where TOptions : class, IOptionsConfiguration
    {
        builder.Services
            .AddOptions<TOptions>()
            .Bind(
                builder.Configuration.GetSection(
                    TOptions.ConfigurationSectionName ?? typeof(TOptions).Name))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return builder;
    }

    public static IHostApplicationBuilder RegisterOptions<TOptions, TValidator>(
        this IHostApplicationBuilder builder)
        where TOptions : class, IOptionsConfiguration
        where TValidator : class, IValidateOptions<TOptions>
    {
        builder.Services
            .AddSingleton<IValidateOptions<TOptions>, TValidator>();

        builder.Services
            .AddOptions<TOptions>()
            .Bind(
                builder.Configuration.GetSection(
                    TOptions.ConfigurationSectionName ?? typeof(TOptions).Name))
            .ValidateOnStart();

        return builder;
    }
}