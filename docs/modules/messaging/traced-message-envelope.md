# Traced Message Envelope

When a raw object is sent via tracing helpers, it can be wrapped in `TracedMessageEnvelope` so tracing context travels with it.

## Types

```csharp
public interface IMessageEnvelope
{
    object Message { get; }
}

public sealed record TracedMessageEnvelope(object Message)
    : IWithTracing, IMessageEnvelope
{
    public string? TraceId { get; set; }
    public string? SpanId { get; set; }
}
```

## Example

```csharp
var envelope = new TracedMessageEnvelope(new PingMessage("hi"));
envelope.AddTracing();
actorRef.Tell(envelope);
```
