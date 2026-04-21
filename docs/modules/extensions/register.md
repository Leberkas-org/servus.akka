# Register Extensions

Register actors in one fluent place during `AddAkka` setup while automatically adding them to Akka.Hosting's registry.

## `WithResolvableActors`

Use the helper when you want to register multiple actor types:

```csharp
builder.Services.AddAkka("servus", akka =>
{
    akka.WithResolvableActors(helper =>
    {
        helper
            .Register<MyActor1>()
            .Register<MyActor2>("custom-name");
    });
});
```

## `WithResolvableActor<TActor>`

Use the direct extension when registering a single actor:

```csharp
builder.Services.AddAkka("servus", akka =>
{
    akka.WithResolvableActor<MyActor>();
});
```

## API

```csharp
public class ActorRegistrationHelper
{
    public ActorRegistrationHelper Register<TActor>(string? name = null, params object[] args)
        where TActor : ActorBase;
}

public static class RegisterExtensions
{
    public static AkkaConfigurationBuilder WithResolvableActors(
        this AkkaConfigurationBuilder builder,
        Action<ActorRegistrationHelper> helper);

    public static AkkaConfigurationBuilder WithResolvableActor<TActor>(
        this AkkaConfigurationBuilder builder,
        string? name = null,
        params object[] args)
        where TActor : ActorBase;
}
```
