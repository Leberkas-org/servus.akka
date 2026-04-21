# ActorRefServiceProvider

A proxy `IServiceProvider` that first delegates to the inner provider, then falls back to creating `ActorRef<TActor>` wrappers for unresolved `IActorRef`-like requests.

## Behavior

1. Try `inner.GetService(serviceType)`.
2. If resolved: return it.
3. If unresolved and request looks like actor-ref generic type: resolve `IActorRegistry`.
4. Create cached constructor for `ActorRef<TActor>` and instantiate it.

## Usage

Normally wired through `ActorRefProviderFactory`, not directly.

## API

```csharp
public class ActorRefServiceProvider : IServiceProvider
{
    public object GetService(Type serviceType);
}
```
