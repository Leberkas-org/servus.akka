# Extensions

The root `Servus.Akka` namespace adds convenience APIs around actor registration, actor resolution, child lookup, and registry access.

## Pages in this section

- [**Register Extensions**](./register) — `WithResolvableActors` and `WithResolvableActor` on `AkkaConfigurationBuilder`.
- [**Resolve Extensions**](./resolve) — DI-backed actor creation from `ActorSystem` or `IActorContext`.
- [**Registry Extensions**](./registry) — typed access to `IActorRegistry` and async actor lookup.
- [**Context Extensions**](./context) — safe child lookup and child tell/forward helpers.
- [**Akka Option Match**](./akka-options) — ergonomic `Option<T>.Match` helpers.

## Namespace map

| Namespace | Types |
|---|---|
| `Servus.Akka` | `ActorRegistrationHelper`, `RegisterExtensions`, `ResolveExtensions`, `RegistryExtensions`, `ContextExtensions`, `AkkaOptionsExtensions` |
