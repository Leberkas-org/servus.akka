# Registry Extensions

Typed shortcuts for `ActorRegistry.For(system)` so actors can be resolved by type from either `ActorSystem` or `IActorContext`.

## Basic usage

```csharp
var actor = actorSystem.GetActor<MyActor>();

if (actorSystem.TryGetActor<MyActor>(out var maybeActor))
{
    maybeActor.Tell("hello");
}

var asyncActor = await actorSystem.GetActorAsync<MyActor>();
```

## API

```csharp
public static class RegistryExtensions
{
    public static IActorRef GetActor<T>(this IActorContext context);
    public static IActorRef GetActor<T>(this ActorSystem system);

    public static bool TryGetActor<T>(this IActorContext context, out IActorRef actor);
    public static bool TryGetActor<T>(this ActorSystem system, out IActorRef actor);

    public static bool TryGetActor(this IActorContext context, Type key, out IActorRef actor);
    public static bool TryGetActor(this ActorSystem system, Type key, out IActorRef actor);

    public static Task<IActorRef> GetActorAsync<T>(this IActorContext context);
    public static Task<IActorRef> GetActorAsync<T>(this ActorSystem system);

    public static IReadOnlyActorRegistry GetRegistry(this ActorSystem system);
}
```
