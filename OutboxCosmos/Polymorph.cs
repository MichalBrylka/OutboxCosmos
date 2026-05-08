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
        options.DerivedTypes.Add(new JsonDerivedType(typeof(TextMessage), "text"));
        options.DerivedTypes.Add(new JsonDerivedType(typeof(ImageMessage), "image"));
        options.DerivedTypes.Add(new JsonDerivedType(typeof(SystemMessage), "system"));
    }
}


public interface IJsonOptionsFactory
{
    JsonSerializerOptions Create();
}

public class JsonOptionsFactory(IEnumerable<IMessageJsonPolymorphicRegistration> polymorphicRegistration) : IJsonOptionsFactory
{
    public JsonSerializerOptions Create()
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
                registration.Register(polymorphism);

            typeInfo.PolymorphismOptions = polymorphism;
        });

        return new JsonSerializerOptions
        {
            //potentially take this from appsettings
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() },

            TypeInfoResolver = resolver,
        };
    }
}