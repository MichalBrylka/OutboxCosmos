using System.Text.Json;
using Xunit.Abstractions;

namespace OutboxCosmos.Tests;

public class MessageSerializationTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private readonly JsonSerializerOptions _options = new JsonOptionsFactory([  new UniversalJsonPolymorphicRegistration() ]).Create();

    [Fact]
    public void Should_serialize_TextMessage_polymorphically()
    {
        // Arrange
        IMessage message = new RFQRequest("123456", "VOD.L", 69M);

        // Act
        var json = JsonSerializer.Serialize(message, _options);
        var expectedJson = """
        {
          "$type": "text",
          "priority": "High",
          "text": "Hello",
          "sourceSession": "session1"
        }
        """;

        // Assert (semantic comparison)
        Normalize(json).Should().Be(Normalize(expectedJson));

        static string Normalize(string json)
        {
            using var doc = JsonDocument.Parse(json);
            return JsonSerializer.Serialize(doc.RootElement);
        }
    }


    public static TheoryData<Type> MessageTypes => [typeof(RFQRequest), typeof(Quote), typeof(QuoteCancel)];

    [Theory]
    [MemberData(nameof(MessageTypes))]
    public void Should_roundtrip_all_message_types(Type messageType)
    {
        var random = new Random(123);

        for (int i = 0; i < 100; i++)
        {
            // Arrange
            var original = MessageFactory.CreateRandomMessage(random, messageType);

            // Act
            string json = JsonSerializer.Serialize(original, _options);
            _output.WriteLine(json);
            IMessage? deserialized = JsonSerializer.Deserialize<IMessage>(json, _options);

            // Assert
            deserialized.Should().NotBeNull();
            deserialized.Should().BeEquivalentTo(original, options => options
                .Using<DateTime>(ctx =>
                {
                    ctx.Subject.Should().BeCloseTo(ctx.Expectation, TimeSpan.FromMilliseconds(1));
                })
                .WhenTypeIs<DateTime>()
            );
        }
    }
}