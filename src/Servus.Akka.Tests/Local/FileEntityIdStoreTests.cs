using Servus.Akka.Local;

namespace Servus.Akka.Tests.Local;

public class FileEntityIdStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"servus-test-{Guid.NewGuid()}");
    private string StorePath => Path.Combine(_directory, "entities.store");

    [Fact]
    public async Task LoadReturnsEmptyWhenFileDoesNotExist()
    {
        var store = new FileEntityIdStore(_directory);
        var entities = await store.LoadEntitiesAsync();
        Assert.Empty(entities);
    }

    [Fact]
    public async Task PersistsEntitiesToFile()
    {
        var store = new FileEntityIdStore(_directory);
        await store.EntityStarted("entity-1");
        await store.EntityStarted("entity-2");

        Assert.True(File.Exists(StorePath));
        var lines = await File.ReadAllLinesAsync(StorePath, TestContext.Current.CancellationToken);
        Assert.Equal(2, lines.Length);
        Assert.Contains("entity-1", lines);
        Assert.Contains("entity-2", lines);
    }

    [Fact]
    public async Task SurvivesNewInstance()
    {
        var store1 = new FileEntityIdStore(_directory);
        await store1.EntityStarted("entity-1");
        await store1.EntityStarted("entity-2");

        var store2 = new FileEntityIdStore(_directory);
        var entities = await store2.LoadEntitiesAsync();

        Assert.Equal(2, entities.Count);
        Assert.Contains("entity-1", entities);
        Assert.Contains("entity-2", entities);
    }

    [Fact]
    public async Task StopRemovesFromFile()
    {
        var store = new FileEntityIdStore(_directory);
        await store.EntityStarted("entity-1");
        await store.EntityStarted("entity-2");
        await store.EntityStopped("entity-1");

        var lines = await File.ReadAllLinesAsync(StorePath, TestContext.Current.CancellationToken);
        Assert.Single(lines);
        Assert.Contains("entity-2", lines);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, true);
    }
}
