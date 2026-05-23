namespace Servus.Akka.Local;

public class InMemoryEntityIdStore : IEntityIdStore
{
    private readonly HashSet<string> _entities = [];

    public Task<IReadOnlyCollection<string>> LoadEntitiesAsync()
        => Task.FromResult<IReadOnlyCollection<string>>(_entities.ToList());

    public Task EntityStarted(string entityId)
    {
        _entities.Add(entityId);
        return Task.CompletedTask;
    }

    public Task EntityStopped(string entityId)
    {
        _entities.Remove(entityId);
        return Task.CompletedTask;
    }
}
