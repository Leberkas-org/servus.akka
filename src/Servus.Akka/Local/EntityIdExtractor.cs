namespace Servus.Akka.Local;

public class EntityIdExtractor : IEntityIdExtractor
{
    public string? EntityId(object message) => (message as IWithEntityId)?.EntityId;

    public object EntityMessage(object message) => message;
}
