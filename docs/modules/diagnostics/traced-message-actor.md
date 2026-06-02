# Traced Message Actor

`TracedMessageActor` is a base actor that unwraps envelopes, opens tracing activities for traced messages, and offers a handler-style API (`Receive`, `ReceiveAsync`, `Become`, `BecomeStacked`).

## Basic example

```csharp
public class TracedPongActor : TracedMessageActor
{
    public TracedPongActor()
    {
        Receive<TracedMessage>(msg => ReplyTraced(new TracedMessage("pong")));
        Receive<int>(_ => ReplyTraced(666));
    }
}
```

## Async handlers

```csharp
ReceiveAsync<TracedMessage>(async msg =>
{
    ReplyTraced(new TracedMessage("pooong"));
    await Task.Delay(1);
});
```

## Behavior switching

```csharp
Receive<string>(s =>
{
    if (s == "stack")
    {
        BecomeStacked(() =>
        {
            Receive<int>(_ => ReplyTraced(69420));
            Receive<string>(_ => UnbecomeStacked());
        });
    }
});
```

## API highlights

```csharp
public class TracedMessageActor : UntypedActor
{
    protected void Become(Action configure);
    protected void BecomeStacked(Action configure);
    protected new void UnbecomeStacked();

    protected void Receive<T>(Action<T> handler, Predicate<T>? shouldHandle = null);
    protected void ReceiveAny(Action<object> handler);

    protected void ReceiveAsync<T>(Func<T, Task> handler, Predicate<T>? shouldHandle = null);
    protected void ReceiveAnyAsync(Func<object, Task> handler);

    protected void ReplyTraced(object message);
    protected void Reply(object message);
}
```
