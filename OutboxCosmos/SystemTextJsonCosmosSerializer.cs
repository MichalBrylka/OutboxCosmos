using Microsoft.Azure.Cosmos;
using System.Text.Json;

namespace PolymorphSerdes;

public class SystemTextJsonCosmosSerializer(JsonSerializerOptions options) : CosmosSerializer
{
    private readonly JsonSerializerOptions _options = options;

    public override T FromStream<T>(Stream stream)
    {
        if (stream == null || stream.Length == 0)
            return default!;

        using (stream)
        {
            return JsonSerializer.Deserialize<T>(stream, _options)!;
        }
    }

    public override Stream ToStream<T>(T input)
    {
        var stream = new MemoryStream();
        JsonSerializer.Serialize(stream, input, _options);
        stream.Position = 0;
        return stream;
    }
}