# Circuit Breaker Extensions

Two extensions that create an Akka `CircuitBreaker` with sensible defaults, removing
the boilerplate of passing `IScheduler` and the three required time parameters every time.

A circuit breaker protects actors that call external services (HTTP APIs, databases,
message brokers) from cascading failures. It transitions through three states:

- **Closed** — calls pass through; failures are counted.
- **Open** — calls fail-fast with `OpenCircuitException`; no load hits the service.
- **Half-Open** — one probe call is let through; success resets, failure re-opens.

## Enrichment actor — PipeTo with typed response

An actor that enriches domain data by calling an external API. The breaker wraps the
async call and pipes the result back. When the API is down, subsequent requests fail
fast instead of queuing up and timing out one by one.

```csharp
public sealed class EnrichmentActor : ReceiveActor
{
    private readonly CircuitBreaker _breaker;
    private readonly TvdbClient _client;

    public EnrichmentActor(TvdbClient client)
    {
        _client = client;
        _breaker = Context.CreateCircuitBreaker(maxFailures: 3,
            callTimeout: TimeSpan.FromSeconds(15),
            configure: b => b.OnOpen(() =>
                Context.GetLogger().Warning("TVDB circuit opened — enrichment paused")));

        Receive<EnrichEpisodes>(msg =>
        {
            _breaker
                .WithCircuitBreaker(ct => _client.GetEpisodesAsync(msg.TvdbId, ct))
                .PipeTo(Sender,
                    success: episodes => new EnrichEpisodesCompleted(msg.TvdbId, episodes),
                    failure: ex => new EnrichEpisodesFailed(msg.TvdbId, ex));
        });
    }
}

public sealed record EnrichEpisodes(int TvdbId);
public abstract record EnrichEpisodesResponse(int TvdbId);
public sealed record EnrichEpisodesCompleted(int TvdbId, IReadOnlyList<Episode> Episodes)
    : EnrichEpisodesResponse(TvdbId);
public sealed record EnrichEpisodesFailed(int TvdbId, Exception Error)
    : EnrichEpisodesResponse(TvdbId);
```

PipeTo's `success` and `failure` mappers turn the raw result into a typed response —
the caller always gets either `Completed` or `Failed`, whether the breaker is closed,
open, or half-open.

## Connection actor — Tell with manual Succeed/Fail

A long-lived actor that manages a connection to an external system (MQTT broker,
database, gRPC stream). The breaker tracks health through response messages and
timeouts rather than wrapping an async call directly.

```csharp
public sealed class MqttPublishActor : ReceiveActor
{
    private readonly CircuitBreaker _breaker;
    private readonly IActorRef _connection;

    public MqttPublishActor(IActorRef connection)
    {
        _connection = connection;
        _breaker = Context.CreateCircuitBreaker(maxFailures: 5,
            resetTimeout: TimeSpan.FromSeconds(30));
        Context.SetReceiveTimeout(TimeSpan.FromSeconds(10));

        Receive<PublishSensorData>(msg =>
        {
            if (_breaker.IsOpen)
            {
                Sender.Tell(new PublishFailed(msg.SensorId, "circuit open — broker unreachable"));
                return;
            }
            _connection.Tell(new MqttPublish(msg.Topic, msg.Payload));
        });

        Receive<MqttPublishAck>(_ => _breaker.Succeed());

        Receive<MqttPublishNack>(nack =>
        {
            _breaker.Fail();
            Context.GetLogger().Warning("MQTT publish rejected: {0}", nack.Reason);
        });

        Receive<ReceiveTimeout>(_ =>
        {
            _breaker.Fail();
            Context.GetLogger().Warning("MQTT publish timed out");
        });
    }
}
```

This pattern works for any protocol where you send a message and expect an ack — the
breaker counts missing or negative acks as failures and opens when the remote side
stops responding.

## Multi-phase workflow — shared breaker

When multiple actors call the same backend, sharing a single `CircuitBreaker` gives
you a unified failure budget. One actor tripping the breaker protects the others from
piling on.

```csharp
builder.Services.AddAkka("search", akka =>
{
    akka.AddStartup((system, registry) =>
    {
        var mediathekBreaker = system.CreateCircuitBreaker(
            maxFailures: 10,
            callTimeout: TimeSpan.FromSeconds(20),
            resetTimeout: TimeSpan.FromMinutes(1),
            configure: b => b
                .WithExponentialBackoff(TimeSpan.FromMinutes(5))
                .WithRandomFactor(0.2)
                .OnOpen(() => Log.Warning("Mediathek API circuit opened"))
                .OnHalfOpen(() => Log.Info("Mediathek API circuit probing...")));

        // Multiple search workers share the same breaker.
        // When one worker trips it, all workers fail fast
        // instead of hammering the API with N parallel timeouts.
        for (var i = 0; i < workerCount; i++)
        {
            system.ActorOf(Props.Create(() =>
                new SearchWorker(mediathekBreaker, mediathekClient)));
        }
    });
});
```

Inside the worker:

```csharp
public sealed class SearchWorker : ReceiveActor
{
    public SearchWorker(CircuitBreaker breaker, MediathekClient client)
    {
        Receive<SearchRequest>(msg =>
        {
            var sender = Sender;
            breaker
                .WithCircuitBreaker(ct => client.SearchAsync(msg.Query, ct))
                .PipeTo(sender, success: results => new SearchCompleted(results),
                    failure: ex => new SearchFailed(ex));
        });
    }
}
```

Without the shared breaker, 10 workers would each burn through 10 timeouts before
individually opening — that's 100 failed requests. With it, the first 10 trip the
breaker and the remaining 90 fail instantly.

## Defaults

Akka's `CircuitBreaker` constructor requires `IScheduler`, `maxFailures`, `callTimeout`
and `resetTimeout` on every call. These extensions supply the scheduler from the
context/system and make the thresholds optional:

| Parameter | Default | Guidance |
|---|---|---|
| `maxFailures` | 5 | 3–10 depending on traffic volume |
| `callTimeout` | 10s | match your SLA or HTTP timeout |
| `resetTimeout` | 30s | how long to stay open before probing |

Need exponential backoff on the reset timeout, random jitter, or state-change callbacks?
Pass `configure` — it runs after the breaker is constructed:

```csharp
var breaker = Context.CreateCircuitBreaker(
    configure: b => b
        .WithExponentialBackoff(TimeSpan.FromMinutes(5))
        .WithRandomFactor(0.2)
        .OnOpen(() => Log.Warning("Circuit opened"))
        .OnClose(() => Log.Info("Circuit closed"))
        .OnHalfOpen(() => Log.Info("Circuit half-open, probing...")));
```

## API

```csharp
public static class CircuitBreakerExtensions
{
    public static CircuitBreaker CreateCircuitBreaker(this IActorContext context,
        int maxFailures = 5,
        TimeSpan? callTimeout = null,
        TimeSpan? resetTimeout = null,
        Func<CircuitBreaker, CircuitBreaker>? configure = null);

    public static CircuitBreaker CreateCircuitBreaker(this ActorSystem system,
        int maxFailures = 5,
        TimeSpan? callTimeout = null,
        TimeSpan? resetTimeout = null,
        Func<CircuitBreaker, CircuitBreaker>? configure = null);
}
```

## When to use Polly instead

If your service call goes through a typed `HttpClient` registered with
`IHttpClientFactory`, Polly's `AddStandardResilienceHandler` is the better fit —
it bundles retry, circuit breaker, timeout, and rate limiter at the HTTP layer and
integrates with `Retry-After` headers out of the box.

Use Servus `CreateCircuitBreaker` when:

- The call happens **inside an actor** and you want to use the **PipeTo** or **Tell** pattern.
- You need to protect a **non-HTTP dependency** (MQTT broker, database, gRPC stream).
- You want a **shared breaker** across multiple actors calling the same backend.
- The actor already handles **its own retry scheduling** (e.g. via `BackoffSupervisor`)
  and you only need fast-fail when the dependency is down.
