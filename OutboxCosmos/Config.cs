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

public class RetryOptions : IOptionsConfiguration
{
    public static string ConfigurationSectionName => "Retry";

    public int MaxAttempts { get; set; }
}

public class MessageRoutingOptions : Dictionary<string, List<string>>, IOptionsConfiguration
{
    public static string ConfigurationSectionName => "MessageRouting";
}

public sealed class MessageRoutingOptionsValidator(IEnumerable<IOutboxMessageHandler> handlers) : IValidateOptions<MessageRoutingOptions>
{
    public ValidateOptionsResult Validate(string? name, MessageRoutingOptions options)
    {
        var errors = new List<string>();
        var uniqueDestinations = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (messageType, destinations) in options)
        {
            if (string.IsNullOrWhiteSpace(messageType))
            {
                errors.Add("Message routing: message type key cannot be empty.");
                continue;
            }

            if (destinations is null || destinations.Count == 0)
            {
                errors.Add($"Message routing: message type '{messageType}' must contain at least one destination.");
                continue;
            }

            if (destinations.Any(string.IsNullOrWhiteSpace))
                errors.Add($"Message type '{messageType}' contains empty destinations.");


            var nonEmptyDestinations = destinations.Where(d => !string.IsNullOrWhiteSpace(d)).ToList();

            var duplicates = nonEmptyDestinations
                .GroupBy(x => x, StringComparer.Ordinal)
                .Where(x => x.Count() > 1)
                .Select(x => x.Key)
                .ToList();

            if (duplicates.Count > 0)
                errors.Add($"Message type '{messageType}' contains duplicate destinations: {string.Join(", ", duplicates.Select(d => $"'{d}'"))}");

            foreach (var d in nonEmptyDestinations)
                uniqueDestinations.Add(d);
        }

        var possibleDestinations = handlers
            .Select(h => h.Name)
            .Where(d => !string.IsNullOrWhiteSpace(d))
            .ToHashSet(StringComparer.Ordinal);

        var unavailableDestinations = uniqueDestinations.Except(possibleDestinations).ToList();

        if (unavailableDestinations.Count > 0)
            errors.Add($"The following destinations are configured but do not have corresponding handlers: {string.Join(", ", unavailableDestinations.Select(d => $"'{d}'"))}");

        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
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