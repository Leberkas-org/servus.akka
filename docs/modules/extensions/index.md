# Extensions

The root `Servus.Akka` namespace adds convenience APIs around actor registration, actor resolution, child lookup, and registry access.

## Pages in this section

- [**Register Extensions**](./register) — `WithResolvableActors` and `WithResolvableActor` on `AkkaConfigurationBuilder`.
- [**Backoff Extensions**](./backoff) — wrap `Props` in a `BackoffSupervisor`, on-failure or on-stop.
- [**Circuit Breaker Extensions**](./circuit-breaker) — create a `CircuitBreaker` with sensible defaults from `IActorContext` or `ActorSystem`.
- [**Resolve Extensions**](./resolve) — DI-backed actor creation from `ActorSystem` or `IActorContext`.
- [**Registry Extensions**](./registry) — typed access to `IActorRegistry` and async actor lookup.
- [**Context Extensions**](./context) — safe child lookup and child tell/forward helpers.
- [**Akka Option Match**](./akka-options) — ergonomic `Option<T>.Match` helpers.

## Namespace map

| Namespace | Types |
|---|---|
| `Servus.Akka` | `ActorRegistrationHelper`, `RegisterExtensions`, `BackoffExtensions`, `CircuitBreakerExtensions`, `ResolveExtensions`, `RegistryExtensions`, `ContextExtensions`, `AkkaOptionsExtensions` |
