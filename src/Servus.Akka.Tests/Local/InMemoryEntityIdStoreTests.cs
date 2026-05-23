using Servus.Akka.Local;

namespace Servus.Akka.Tests.Local;

public class InMemoryEntityIdStoreTests
{
    private readonly InMemoryEntityIdStore _store = new();

    [Fact]
    public async Task LoadReturnsEmptyInitially()
    {
        var entities = await _store.LoadEntitiesAsync();
        Assert.Empty(entities);
    }

    [Fact]
    public async Task StartedEntitiesAreLoadable()
    {
        await _store.EntityStarted("entity-1");
        await _store.EntityStarted("entity-2");

        var entities = await _store.LoadEntitiesAsync();
        Assert.Equal(2, entities.Count);
        Assert.Contains("entity-1", entities);
        Assert.Contains("entity-2", entities);
    }

    [Fact]
    public async Task StoppedEntitiesAreRemoved()
    {
        await _store.EntityStarted("entity-1");
        await _store.EntityStarted("entity-2");
        await _store.EntityStopped("entity-1");

        var entities = await _store.LoadEntitiesAsync();
        Assert.Single(entities);
        Assert.Contains("entity-2", entities);
    }

    [Fact]
    public async Task DuplicateStartIsIdempotent()
    {
        await _store.EntityStarted("entity-1");
        await _store.EntityStarted("entity-1");

        var entities = await _store.LoadEntitiesAsync();
        Assert.Single(entities);
    }
}
