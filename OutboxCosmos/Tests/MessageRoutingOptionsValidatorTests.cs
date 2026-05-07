using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace OutboxCosmos.Tests;

public sealed class MessageRoutingOptionsValidatorTests
{
    private static readonly FakeHandler OrdersHandler = new("orders");
    private static readonly FakeHandler BillingHandler = new("billing");
    private static readonly FakeHandler NotificationsHandler = new("notifications");

    [Fact]
    public void Validate_ShouldReturnSuccess_WhenConfigurationIsValid()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders", "billing"],
            ["UserRegistered"] = ["notifications"]
        };

        var validator = CreateValidator(
            OrdersHandler,
            BillingHandler,
            NotificationsHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
        result.Failed.Should().BeFalse();
    }

    [Fact]
    public void Validate_ShouldFail_WhenMessageTypeKeyIsEmpty()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            [""] = ["orders"]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("Message routing: message type key cannot be empty.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenMessageTypeKeyIsWhitespace()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["   "] = ["orders"]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .Contain("Message routing: message type key cannot be empty.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDestinationsAreNull()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = null!
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("Message routing: message type 'OrderCreated' must contain at least one destination.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDestinationsAreEmpty()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = []
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("Message routing: message type 'OrderCreated' must contain at least one destination.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDestinationContainsEmptyString()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders", ""]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("Message type 'OrderCreated' contains empty destinations.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDestinationContainsWhitespace()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders", "   "]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .Contain("Message type 'OrderCreated' contains empty destinations.");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDuplicateDestinationsExist()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders", "orders"]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("Message type 'OrderCreated' contains duplicate destinations: 'orders'");
    }

    [Fact]
    public void Validate_ShouldFail_WhenDestinationDoesNotHaveHandler()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders", "missing-handler"]
        };

        var validator = CreateValidator(OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle()
            .Which.Should()
            .Be("The following destinations are configured but do not have corresponding handlers: 'missing-handler'");
    }

    [Fact]
    public void Validate_ShouldFail_WhenMultipleDestinationsDoNotHaveHandlers()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["missing-1", "missing-2"]
        };

        var validator = CreateValidator();

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .ContainSingle(x =>
                x.Contains("missing-1") &&
                x.Contains("missing-2"));
    }

    [Fact]
    public void Validate_ShouldIgnoreHandlerNamesThatAreNullOrWhitespace()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["orders"]
        };

        var validator = CreateValidator(
            new FakeHandler(""),
            new FakeHandler(" "),
            OrdersHandler);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_ShouldTreatHandlerNamesCaseSensitive()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            ["OrderCreated"] = ["ORDERS"]
        };

        var validator = CreateValidator(
            new FakeHandler("orders"));

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should()
            .Contain("The following destinations are configured but do not have corresponding handlers: 'ORDERS'");
    }

    [Fact]
    public void Validate_ShouldReturnAllValidationErrors()
    {
        // Arrange
        var options = new MessageRoutingOptions
        {
            [""] = ["orders"],
            ["OrderCreated"] = ["orders", "", "orders"],
            ["UserRegistered"] = []
        };

        var validator = CreateValidator(
            new FakeHandler("orders"));

        // Act
        var result = validator.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();

        result.Failures.Should().HaveCount(4);

        result.Failures.Should().Contain(
            "Message routing: message type key cannot be empty.");

        result.Failures.Should().Contain(
            "Message type 'OrderCreated' contains empty destinations.");

        result.Failures.Should().Contain(x =>
            x.StartsWith("Message type 'OrderCreated' contains duplicate destinations:"));

        result.Failures.Should().Contain(
            "Message routing: message type 'UserRegistered' must contain at least one destination.");
    }

    private static MessageRoutingOptionsValidator CreateValidator(params IOutboxMessageHandler[] handlers) => new MessageRoutingOptionsValidator(handlers);

    private sealed class FakeHandler(string name) : IOutboxMessageHandler
    {
        public string Name { get; } = name;

        public Task Publish(string id, IMessage message) => Task.CompletedTask;
    }

}