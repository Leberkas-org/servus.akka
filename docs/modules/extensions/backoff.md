# Backoff Extensions

Two extensions on `Props` that wrap an actor in Akka's `BackoffSupervisor`, with
sensible defaults for the parameters Akka itself requires on every call:

- **`BackoffOnFailure`** — restart only when the child crashes with an unhandled exception.
- **`BackoffOnStop`** — restart whenever the child stops, including a graceful stop.

Both return `Props`, so they drop straight into anywhere you already build one — a
plain `WithActors` callback, or a cluster `WithSingleton` factory:

```csharp
builder.WithSingleton<IMediathekManager>("mediathek-view-web-manager",
    (_, _, resolver) => resolver.Props<MediathekViewWebManager>()
        .BackoffOnFailure("mediathek-view-web-manager"));

builder.WithActors((system, registry) =>
{
    var resolver = DependencyResolver.For(system);
    var props = resolver.Props<MyAlwaysOnActor>().BackoffOnStop("my-always-on-actor");

    registry.Register<MyAlwaysOnActor>(system.ActorOf(props, "my-always-on-actor-supervisor"));
});
```

## Defaults

Akka's own `Backoff.OnFailure`/`OnStop` require `minBackoff`, `maxBackoff` and
`randomFactor` on every call — these extensions make them optional:

| Parameter | Default | Guidance |
|---|---|---|
| `minBackoff` | 3s | 1–3s |
| `maxBackoff` | 30s | 30–60s |
| `randomFactor` | 0.2 | 0.1–0.3 jitter, prevents thundering herd |

Need `WithMaxNrOfRetries`, `WithManualReset`, `WithSupervisorStrategy`, or another of
Akka's own fluent `BackoffOptions` methods? Pass `configure` — it runs after the
defaults above are applied and before the supervisor `Props` are built:

```csharp
resolver.Props<MyPersistentActor>()
    .BackoffOnFailure("my-persistent-actor", configure: o => o.WithMaxNrOfRetries(5));
```

## API

```csharp
public static class BackoffExtensions
{
    public static Props BackoffOnFailure(this Props childProps, string childName,
        TimeSpan? minBackoff = null, TimeSpan? maxBackoff = null, double randomFactor = 0.2,
        Func<BackoffOptions, BackoffOptions>? configure = null);

    public static Props BackoffOnStop(this Props childProps, string childName,
        TimeSpan? minBackoff = null, TimeSpan? maxBackoff = null, double randomFactor = 0.2,
        Func<BackoffOptions, BackoffOptions>? configure = null);
}
```
