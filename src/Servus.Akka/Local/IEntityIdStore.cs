namespace Servus.Akka.Local;

public interface IEntityIdStore
{
    Task<IReadOnlyCollection<string>> LoadEntitiesAsync();
    Task EntityStarted(string entityId);
    Task EntityStopped(string entityId);
}
