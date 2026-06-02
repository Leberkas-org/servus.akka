# ActorRef\<TActor\>

`ActorRef<TActor>` is a typed wrapper implementing `IActorRef` that resolves the backing actor from `IActorRegistry`.

## Why use it

It lets you request actors via DI with a concrete actor type:

```csharp
public class MessagePump(ActorRef<MyActor> actor)
{
    public void Send(object message)
    {
        actor.Tell(message, ActorRefs.NoSender);
    }
}
```

## API

```csharp
public sealed class ActorRef<TActor> : IActorRef
    where TActor : ActorBase
{
    public void Tell(object message, IActorRef sender);
    public ActorPath Path { get; }
}
```
