using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace OutboxCosmos;


public interface IMessageJsonPolymorphicRegistration
{
    void Register(JsonPolymorphismOptions options);
}

public sealed class UniversalJsonPolymorphicRegistration : IMessageJsonPolymorphicRegistration
{
    public void Register(JsonPolymorphismOptions options)
    {
        options.DerivedTypes.Add(new JsonDerivedType(typeof(RFQRequest), "RfqRequest"));
        options.DerivedTypes.Add(new JsonDerivedType(typeof(Quote), "Quote"));
        options.DerivedTypes.Add(new JsonDerivedType(typeof(QuoteCancel), "QuoteCancel"));
    }
}


public interface IJsonOptionsFactory
{
    JsonSerializerOptions Create();
}

public class DefaultJsonOptionsFactory : IJsonOptionsFactory
{
    public JsonSerializerOptions Create()
    {
        var options = CreateDefaultOptions();

        Configure(options);

        return options;
    }

    protected virtual JsonSerializerOptions CreateDefaultOptions()
    {
        return new JsonSerializerOptions
        {
            // potentially take this from appsettings
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    protected virtual void Configure(JsonSerializerOptions options) { }
}

public class JsonOptionsFactory(IEnumerable<IMessageJsonPolymorphicRegistration> polymorphicRegistration) : DefaultJsonOptionsFactory
{
    protected override void Configure(JsonSerializerOptions options)
    {
        var resolver = new DefaultJsonTypeInfoResolver();

        resolver.Modifiers.Add(typeInfo =>
        {
            if (typeInfo.Type != typeof(IMessage)) return;

            var polymorphism = new JsonPolymorphismOptions
            {
                TypeDiscriminatorPropertyName = "$type",
                IgnoreUnrecognizedTypeDiscriminators = false,
                UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization
            };

            foreach (var registration in polymorphicRegistration)
            {
                registration.Register(polymorphism);
            }

            typeInfo.PolymorphismOptions = polymorphism;
        });

        options.TypeInfoResolver = resolver;
    }
}