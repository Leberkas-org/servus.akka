# ActorRefProviderFactory

`IServiceProviderFactory<IServiceCollection>` implementation that creates an `ActorRefServiceProvider` wrapper around the built provider.

## Usage

```csharp
builder.Host.UseServiceProviderFactory(new ActorRefProviderFactory());
```

This keeps default DI behavior, then adds actor-ref fallback support through the wrapper.

## API

```csharp
public class ActorRefProviderFactory : IServiceProviderFactory<IServiceCollection>
{
    public IServiceCollection CreateBuilder(IServiceCollection services);
    public IServiceProvider CreateServiceProvider(IServiceCollection containerBuilder);
}
```
