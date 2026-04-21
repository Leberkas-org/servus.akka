# Resolve Extensions

Create actors through Akka.DependencyInjection without repeating `DependencyResolver.For(system)` plumbing.

## Resolve from `ActorSystem`

```csharp
var actor = actorSystem.ResolveActor<MyActor>();
var namedActor = actorSystem.ResolveActor<MyActor>("my-actor");
```

## Resolve from `IActorContext`

```csharp
var sibling = Context.ResolveActor<WorkerActor>();
var child = Context.ResolveChildActor<WorkerActor>("child-worker");
```

## API

```csharp
public static class ResolveExtensions
{
    public static IActorRef ResolveChildActor<TActor>(
        this IActorContext context,
        string? name,
        params object[] args)
        where TActor : ActorBase;

    public static IActorRef ResolveChildActor<TActor>(
        this IActorContext context,
        params object[] args)
        where TActor : ActorBase;

    public static IActorRef ResolveActor<TActor>(
        this IActorContext context,
        string? name,
        params object[] args)
        where TActor : ActorBase;

    public static IActorRef ResolveActor<TActor>(
        this ActorSystem system,
        string? name,
        params object[] args)
        where TActor : ActorBase;
}
```
