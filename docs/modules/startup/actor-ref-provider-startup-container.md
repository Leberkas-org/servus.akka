# ActorRefProviderStartupContainer

Host-builder setup container that installs `ActorRefProviderFactory` so `ActorRef<TActor>` can be resolved from DI.

## Usage

```csharp
var app = AppBuilder
    .Create()
    .WithSetup<ActorRefProviderStartupContainer>()
    .WithSetup<MyActorSystemSetupContainer>()
    .Build();
```

## API

```csharp
public class ActorRefProviderStartupContainer : IHostBuilderSetupContainer
{
    public void ConfigureHostBuilder(IHostBuilder builder);
}
```
