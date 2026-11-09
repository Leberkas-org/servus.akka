# ActorRefServiceProvider

A proxy `IServiceProvider` that first delegates to the inner provider, then falls back to creating `ActorRef<TActor>` wrappers from the `IActorRegistry`.

## Behavior

1. Try `inner.GetService(serviceType)`.
2. If resolved: return it.
3. If unresolved and the type is `ActorRef<TActor>`: create it from the `IActorRegistry` (constructors are cached, thread-safe).
4. Any other unresolved type returns `null`, like the inner provider.

Scopes created through the provider (for example ASP.NET Core request services) resolve `ActorRef<TActor>` too, `IServiceProviderIsService` reports `ActorRef<TActor>` as a service, keyed services are forwarded to the inner provider, and disposing the provider disposes the inner one.

## Usage

Normally wired through `ActorRefProviderFactory`, not directly.

## API

```csharp
public class ActorRefServiceProvider
    : IKeyedServiceProvider, IServiceProviderIsService, IDisposable, IAsyncDisposable
{
    public object? GetService(Type serviceType);
    public object? GetKeyedService(Type serviceType, object? serviceKey);
    public object GetRequiredKeyedService(Type serviceType, object? serviceKey);
    public bool IsService(Type serviceType);
}
```
