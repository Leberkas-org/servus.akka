namespace Servus.Akka.Local;

public interface IEntityIdExtractor
{
    string? EntityId(object message);
    object EntityMessage(object message);
}
