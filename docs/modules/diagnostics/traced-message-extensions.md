# Traced Message Extensions

Extensions for `ICanTell` and `IActorRef` that ensure messages carry tracing context (`TraceId`, `SpanId`).

## `TellTraced`

```csharp
recipient.TellTraced(new OrderAccepted(orderId));
recipient.TellTraced(rawPayloadObject); // wrapped in TracedMessageEnvelope
```

## `AskTraced<T>`

```csharp
var response = await actorRef.AskTraced<OrderResult>(new OrderRequested(id));
```

## `ForwardTraced`

```csharp
recipient.ForwardTraced(message);
```

## API

```csharp
public static class TracedActorMessageExtensions
{
    public static void TellTraced(this ICanTell recipient, object message, IActorRef? sender = null);
    public static void TellTraced(this ICanTell recipient, IWithTracing message, IActorRef? sender = null);

    public static Task<T> AskTraced<T>(this IActorRef recipient, object message);
    public static Task<T> AskTraced<T>(this IActorRef recipient, IWithTracing message);

    public static void ForwardTraced(this IActorRef recipient, object message);
    public static void ForwardTraced(this IActorRef recipient, IWithTracing message);
}
```
