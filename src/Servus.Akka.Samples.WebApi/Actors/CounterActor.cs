using Akka.Actor;
using Servus.Akka.Local;

namespace Servus.Akka.Samples.WebApi.Actors;

public sealed record Increment(string EntityId) : IWithEntityId;
public sealed record Decrement(string EntityId) : IWithEntityId;
public sealed record GetCount(string EntityId) : IWithEntityId;
public sealed record CounterValue(string EntityId, int Count);

public class CounterActor : ReceiveActor
{
    private int _count;

    public CounterActor(string entityId)
    {
        Receive<Increment>(_ =>
        {
            _count++;
            Sender.Tell(new CounterValue(entityId, _count));
        });

        Receive<Decrement>(_ =>
        {
            _count--;
            Sender.Tell(new CounterValue(entityId, _count));
        });

        Receive<GetCount>(_ =>
        {
            Sender.Tell(new CounterValue(entityId, _count));
        });
    }
}
