# ActorSystemSetupContainer

Abstract setup container that wires `services.AddAkka(...)` into Servus.Core startup container flow.

## Intended usage

Inherit it in your application and provide:

1. actor-system name
2. Akka system configuration callback

```csharp
public sealed class MyActorSystemSetupContainer : ActorSystemSetupContainer
{
    protected override string GetActorSystemName() => "my-system";

    protected override void BuildSystem(
        AkkaConfigurationBuilder builder,
        IServiceProvider serviceProvider)
    {
        builder.WithResolvableActor<MyRootActor>();
    }
}
```

## API

```csharp
public abstract class ActorSystemSetupContainer : IServiceSetupContainer
{
    public void SetupServices(IServiceCollection services, IConfiguration configuration);

    protected abstract string GetActorSystemName();
    protected abstract void BuildSystem(
        AkkaConfigurationBuilder builder,
        IServiceProvider serviceProvider);
}
```
