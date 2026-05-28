namespace Servus.Akka.TestKit;

public sealed class BehaviorStack<TIn, TOut>(Func<TIn, TOut> defaultBehavior)
{
    private readonly Stack<Func<TIn, TOut>> _stack = new();

    public void Push(Func<TIn, TOut> behavior) => _stack.Push(behavior);

    public void PushConstant(TOut value) => Push(_ => value);

    public void PushError(Exception exception) => Push(_ => throw exception);

    public DelayGate<TIn, TOut> PushDelayed()
    {
        var gate = new DelayGate<TIn, TOut>();
        Push(gate.Execute);
        return gate;
    }

    public void PushOnce(Func<TIn, TOut> behavior)
    {
        Push(input =>
        {
            Pop();
            return behavior(input);
        });
    }

    public void Pop() => _stack.TryPop(out _);

    public TOut Apply(TIn input)
    {
        if (_stack.TryPeek(out var behavior))
        {
            return behavior(input);
        }

        return defaultBehavior(input);
    }
}

public sealed class DelayGate<TIn, TOut>
{
    private readonly TaskCompletionSource<TOut> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal TOut Execute(TIn _) => _tcs.Task.GetAwaiter().GetResult();

    public void Release(TOut value) => _tcs.TrySetResult(value);

    public void Fault(Exception exception) => _tcs.TrySetException(exception);
}
