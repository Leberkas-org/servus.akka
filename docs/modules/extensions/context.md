# Context Extensions

Helpers that make child lookup and child messaging safer and easier to read.

## Child lookup and safe send

```csharp
public class ParentActor : ReceiveActor
{
    public ParentActor()
    {
        Context.ResolveChildActor<WorkerActor>("worker");

        Receive<Work>(work =>
        {
            var sent = Context.ChildTell("worker", work);
            if (!sent) Sender.Tell(Status.Failure(new InvalidOperationException("worker missing")));
        });
    }
}
```

`ChildTell` / `ChildForward` return `false` when the child does not exist.

## Traced variants

If your message implements `IWithTracing`, traced variants stamp trace context before forwarding/sending:

```csharp
Context.ChildTellTraced("worker", message);
Context.ChildForwardTraced("worker", message);
```

## API

```csharp
public static class ContextExtensions
{
    public static Option<IActorRef> GetChild(this IActorContext context, string name);

    public static bool ChildTell(this IActorContext context, string name, object message);
    public static bool ChildTellTraced(this IActorContext context, string name, IWithTracing message);

    public static bool ChildForward(this IActorContext context, string name, object message);
    public static bool ChildForwardTraced(this IActorContext context, string name, IWithTracing message);
}
```
