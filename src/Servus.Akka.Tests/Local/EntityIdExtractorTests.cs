using Servus.Akka.Local;

namespace Servus.Akka.Tests.Local;

public class EntityIdExtractorTests
{
    private readonly EntityIdExtractor _extractor = new();

    [Fact]
    public void ReturnsEntityIdFromIWithEntityIdMessage()
    {
        var message = new TestEntityMessage("entity-1", "hello");
        Assert.Equal("entity-1", _extractor.EntityId(message));
    }

    [Fact]
    public void ReturnsNullForMessageWithoutIWithEntityId()
    {
        Assert.Null(_extractor.EntityId("plain string"));
    }

    [Fact]
    public void EntityMessageReturnsMessageAsIs()
    {
        var message = new TestEntityMessage("entity-1", "hello");
        Assert.Same(message, _extractor.EntityMessage(message));
    }

    public sealed record TestEntityMessage(string EntityId, string Payload) : IWithEntityId;
}
