# Dependency Injection

Bridge Akka.Hosting actor registry to `IServiceProvider` so typed actor refs can be resolved directly from DI.

## Pages in this section

- [**ActorRef&lt;TActor&gt;**](./actor-ref) — typed `IActorRef` wrapper resolved from `IActorRegistry`.
- [**ActorRefProviderFactory**](./actor-ref-provider-factory) — service-provider factory that wraps the default provider.
- [**ActorRefServiceProvider**](./actor-ref-service-provider) — fallback logic that creates `ActorRef<TActor>` on demand.

## Namespace map

| Namespace | Types |
|---|---|
| `Servus.Akka.DependencyInjection` | `ActorRef<TActor>`, `ActorRefProviderFactory`, `ActorRefServiceProvider` |
