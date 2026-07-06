using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

/// <summary>
/// Controllable <see cref="IDuplexConnection"/> fake for state-machine and pool unit tests. Receive,
/// enqueue, quiesce and flush are all externally driven so a test can observe or gate each in
/// isolation without a real socket/stream.
/// </summary>
internal sealed class FakeDuplexConnection : IDuplexConnection
{
    private TaskCompletionSource<WireBuffer?> _receiveTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<bool> _quiesceTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Action<int>? OnFlushed { get; set; }

    /// <summary>Buffers handed to <see cref="TryEnqueue"/>, in order.</summary>
    public List<WireBuffer> Enqueued { get; } = [];

    /// <summary>When false, <see cref="TryEnqueue"/> returns false and does NOT record the buffer.</summary>
    public bool EnqueueReturns { get; set; } = true;

    public int QuiesceCallCount { get; private set; }
    public bool Disposed { get; private set; }

    public ValueTask<WireBuffer?> ReceiveAsync() => new(_receiveTcs.Task);

    /// <summary>Completes the pending receive and arms the next one.</summary>
    public void CompleteReceive(WireBuffer? buffer)
    {
        var tcs = _receiveTcs;
        _receiveTcs = new TaskCompletionSource<WireBuffer?>(TaskCreationOptions.RunContinuationsAsynchronously);
        tcs.TrySetResult(buffer);
    }

    public bool TryEnqueue(WireBuffer buffer)
    {
        if (!EnqueueReturns)
        {
            return false;
        }

        Enqueued.Add(buffer);
        return true;
    }

    public ValueTask<bool> QuiesceAsync()
    {
        QuiesceCallCount++;
        return new ValueTask<bool>(_quiesceTcs.Task);
    }

    /// <summary>Settles the pending quiesce: true = parked clean (reusable), false = dirty (dispose).</summary>
    public void CompleteQuiesce(bool clean) => _quiesceTcs.TrySetResult(clean);

    /// <summary>Invokes the send-loop flush callback the SM assigned, as the real loop would.</summary>
    public void InvokeFlushed(int bytes) => OnFlushed?.Invoke(bytes);

    public Task CompleteAndDrainOutputAsync() => Task.CompletedTask;

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
