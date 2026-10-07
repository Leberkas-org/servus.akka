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

## Configuring the resolved `Props`

Pass a `Func<Props, Props>` to adjust the DI-resolved `Props` before the actor is
created — attach a router, dispatcher, or mailbox:

```csharp
Context.ResolveChildActor<WorkerActor>("worker-pool",
    props => props.WithRouter(new SmallestMailboxPool(2)));

Sys.ResolveActor<WorkerActor>(configure: props => props.WithDispatcher("my-dispatcher"));
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

    public static IActorRef ResolveChildActor<TActor>(
        this IActorContext context,
        string? name,
        Func<Props, Props> configure,
        params object[] args)
        where TActor : ActorBase;

    public static IActorRef ResolveChildActor<TActor>(
        this IActorContext context,
        Func<Props, Props> configure,
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

    public static IActorRef ResolveActor<TActor>(
        this ActorSystem system,
        string? name,
        Func<Props, Props> configure,
        params object[] args)
        where TActor : ActorBase;

    public static IActorRef ResolveActor<TActor>(
        this ActorSystem system,
        Func<Props, Props> configure,
        params object[] args)
        where TActor : ActorBase;
}
```
