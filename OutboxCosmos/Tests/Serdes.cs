using System.Text.Json;
using System.Text.Json.Serialization;


namespace OutboxCosmos.Tests;

public class MessageJsonConverter(IEnumerable<Type> messageTypes) : JsonConverter<IMessage>
{
    private const string DISCRIMINATOR = "$type";

    private readonly Dictionary<string, Type> _typeMap = messageTypes.ToDictionary(t => t.Name, t => t);
    private readonly Dictionary<Type, string> _reverseTypeMap = messageTypes.ToDictionary(t => t, t => t.Name);

    public override IMessage? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;

        if (!root.TryGetProperty(DISCRIMINATOR, out var typeProperty))
            throw new JsonException($"Missing {DISCRIMINATOR} discriminator");

        var typeName = typeProperty.GetString();

        if (typeName == null || !_typeMap.TryGetValue(typeName, out var targetType))
            throw new JsonException($"Unknown type: {typeName}");

        // Deserialize into concrete type
        var json = root.GetRawText();
        return (IMessage?)JsonSerializer.Deserialize(json, targetType, options);
    }

    public override void Write(Utf8JsonWriter writer, IMessage value, JsonSerializerOptions options)
    {
        var type = value.GetType();

        if (!_reverseTypeMap.TryGetValue(type, out var typeName))
            throw new JsonException($"Unregistered type: {type.Name}");

        writer.WriteStartObject();

        writer.WriteString(DISCRIMINATOR, typeName);

        // Serialize properties
        var json = JsonSerializer.SerializeToElement(value, type, options);

        foreach (var property in json.EnumerateObject())
            property.WriteTo(writer);

        writer.WriteEndObject();
    }
}