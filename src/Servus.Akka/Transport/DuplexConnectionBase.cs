using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// Shared base for full-duplex byte connections. Owns the inbound receive-guard (reentrancy + quiesce
/// settlement via a zero-alloc <see cref="ManualResetValueTaskSourceCore{TResult}"/>), the adaptive
/// receive-hint, the outbound <see cref="Channel{T}"/>, the send-loop skeleton, and the
/// <see cref="DisposeAsync"/> lifecycle. Cross-thread signaling flows through
/// <see cref="CancellationTokenSource.CancelAsync"/> and the settle source's happens-before edge (Reset
/// on the actor thread, SetResult on the I/O thread) — the sole exception is the queued-bytes counter
/// (<see cref="TryEnqueue"/> / <see cref="ReleaseQueuedBytes"/>), a genuine producer (actor thread) /
/// send-loop (I/O thread) system boundary, which uses <see cref="Interlocked"/> deliberately.
/// </summary>
internal abstract class DuplexConnectionBase : IDuplexConnection, IValueTaskSource<bool>
{
    private const int MaxBatchSize = 64;

    private readonly Channel<WireBuffer> _channel;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _lifetimeCts;
    private readonly long _queuedByteCap;

    private CancellationTokenSource _receiveCts;
    private ManualResetValueTaskSourceCore<bool> _settleSource;
    private bool _receiveActive;
    private bool _completed;
    private int _receiveHint;
    private int _shrinkStreak;
    private long _queuedBytes;

    public Action<int>? OnFlushed { get; set; }

    protected int ReceiveHint => _receiveHint;

    /// <summary>
    /// Test-only view of the outstanding outbound queued-byte counter (bytes reserved by
    /// <see cref="TryEnqueue"/> minus bytes released once the send loop drains their buffers). Must land
    /// back at 0 once a connection has fully torn down / aborted, regardless of how many batches were
    /// queued. Read via <see cref="Interlocked"/> for a coherent snapshot across the I/O thread.
    /// </summary>
    internal long QueuedBytesForTest => Interlocked.Read(ref _queuedBytes);

    /// <param name="queuedByteCap">
    /// Outbound queued-bytes cap. <c>&lt;= 0</c> disables the cap (TCP — shared by H2/H1 whose in-flight
    /// byte count legitimately has no comparable tight bound). A positive value tracks bytes enqueued via
    /// <see cref="TryEnqueue"/> minus bytes drained by the send loop (QUIC-per-stream safety net) —
    /// <see cref="TryEnqueue"/> returns false and fails loud when the running total would exceed the cap
    /// while still active, which indicates broken outbound credit accounting upstream. The channel itself
    /// is always unbounded (item count never gates); byte tracking is the sole enforcement so it can never
    /// false-trip purely from a small configured chunk size producing more (smaller) enqueued items.
    /// </param>
    protected DuplexConnectionBase(int receiveBufferHint, long queuedByteCap, Task? sendLoopStartGate = null)
    {
        _receiveHint = receiveBufferHint;
        _receiveCts = new CancellationTokenSource();
        _lifetimeCts = new CancellationTokenSource();
        _queuedByteCap = queuedByteCap;
        _settleSource.RunContinuationsAsynchronously = true;

        _channel = Channel.CreateUnbounded<WireBuffer>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });

        var ct = _lifetimeCts.Token;

        // Do NOT pass `ct` as the second argument to Task.Run: if DisposeAsync cancels the token before
        // the thread pool starts the delegate, Task.Run(f, ct) cancels the task without ever running the
        // body, so the loop's teardown catch + finally never run and the await surfaces a
        // TaskCanceledException. The loop already observes `ct` internally.
        _sendLoop = Task.Run(() => RunSendLoopAsync(_channel.Reader, ct, sendLoopStartGate));
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _settleSource.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _settleSource.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _settleSource.OnCompleted(continuation, state, token, flags);

    public async ValueTask<WireBuffer?> ReceiveAsync()
    {
        if (_receiveActive)
        {
            throw new InvalidOperationException(
                "Concurrent ReceiveAsync — the connection supports one outstanding receive.");
        }

        _receiveActive = true;
        _settleSource.Reset();
        var cancelled = false;

        try
        {
            var buffer = await ReceiveDataAsync(_receiveCts.Token);
            if (buffer is null)
            {
                return null;
            }

            AdaptiveHint.Adapt(buffer.Length, ref _receiveHint, ref _shrinkStreak);
            return buffer;
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
            throw;
        }
        finally
        {
            _receiveActive = false;
            _settleSource.SetResult(cancelled);
        }
    }

    /// <summary>
    /// Performs the actual I/O read. Returns a buffer with <see cref="WireBuffer.Length"/> set on success,
    /// null on EOF. Must dispose the buffer before throwing on error or cancellation.
    /// </summary>
    protected abstract ValueTask<WireBuffer?> ReceiveDataAsync(CancellationToken ct);

    public bool TryEnqueue(WireBuffer buffer)
    {
        // Cross-thread producer/send-loop counter — a genuine system boundary, so Interlocked (not a
        // plain field) is required even under the repo's actor-confinement threading model.
        if (_queuedByteCap > 0)
        {
            var queued = Interlocked.Add(ref _queuedBytes, buffer.Length);
            if (queued > _queuedByteCap)
            {
                Interlocked.Add(ref _queuedBytes, -buffer.Length);

                if (!_completed)
                {
                    // Queued-but-unflushed bytes exceed the cap while the writer is still open — the
                    // outbound byte-credit gate should have parked the producer long before this.
                    // Reaching here is a credit-accounting bug, never a legitimately-credited chunk size.
                    Tracing.For("Transport").Error(this,
                        "Outbound queued bytes ({0}) exceed the {1}-byte cap while active — failing connection; this indicates broken outbound credit accounting.",
                        queued, _queuedByteCap);
                }

                return false;
            }
        }

        if (_channel.Writer.TryWrite(buffer))
        {
            return true;
        }

        // The channel is always unbounded — TryWrite only fails once the writer has completed.
        if (_queuedByteCap > 0)
        {
            Interlocked.Add(ref _queuedBytes, -buffer.Length);
        }

        return false;
    }

    /// <remarks>
    /// Single-caller contract: <see cref="QuiesceAsync"/> and <see cref="DisposeAsync"/> are never invoked
    /// concurrently with each other or with themselves. Plain field writes are safe under this contract;
    /// the settle source's completion provides the happens-before edge between the I/O thread
    /// (ReceiveAsync finally) and the actor thread (QuiesceAsync continuation).
    /// </remarks>
    public async ValueTask<bool> QuiesceAsync()
    {
        var current = _receiveCts;
        await current.CancelAsync();

        var clean = _receiveActive
            ? await new ValueTask<bool>(this, _settleSource.Version)
            : true;

        current.Dispose();
        _receiveCts = new CancellationTokenSource();

        return clean;
    }

    public async Task CompleteAndDrainOutputAsync()
    {
        _completed = true;
        _channel.Writer.TryComplete();
        await _sendLoop;
    }

    /// <summary>
    /// When overridden, returns true to abort the send loop before writing the current batch (e.g. QUIC
    /// <c>WritesClosed</c>). The base disposes the batch, fires <see cref="OnFlushed"/> with 0, and breaks.
    /// </summary>
    protected virtual bool ShouldAbortSendLoop() => false;

    /// <summary>Writes one drained batch to the transport. Returns the total bytes written.</summary>
    protected abstract ValueTask<int> WriteBatchAsync(List<WireBuffer> batch, CancellationToken ct);

    /// <summary>
    /// Releases bytes previously reserved by <see cref="TryEnqueue"/> once the send loop has drained the
    /// owning buffers — the same producer/send-loop cross-thread counter, so <see cref="Interlocked"/> is
    /// required. Called from every drain path: after a normal <see cref="WriteBatchAsync"/>, on the
    /// <see cref="ShouldAbortSendLoop"/> abort branch (current batch), in the teardown catch (current
    /// batch), and in the send-loop <c>finally</c> for any items still queued when the loop exited (the
    /// abort branch's remainder, and buffers left behind when <see cref="DisposeAsync"/> cancels the
    /// lifetime CTS before completing the writer). No-op when the byte cap is disabled
    /// (<c>_queuedByteCap &lt;= 0</c>, e.g. TCP), keeping that path free of the extra atomic op.
    /// </summary>
    private void ReleaseQueuedBytes(int bytes)
    {
        if (_queuedByteCap > 0 && bytes != 0)
        {
            Interlocked.Add(ref _queuedBytes, -bytes);
        }
    }

    private async Task RunSendLoopAsync(
        ChannelReader<WireBuffer> reader, CancellationToken ct, Task? startGate)
    {
        var batch = new List<WireBuffer>(MaxBatchSize);

        try
        {
            if (startGate is not null)
            {
                await startGate;
            }

            while (await reader.WaitToReadAsync(ct))
            {
                batch.Clear();
                while (batch.Count < MaxBatchSize && reader.TryRead(out var item))
                {
                    batch.Add(item);
                }

                if (batch.Count == 0)
                {
                    continue;
                }

                if (ShouldAbortSendLoop())
                {
                    var abortedBytes = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        abortedBytes += batch[i].Length;
                        batch[i].Dispose();
                    }

                    batch.Clear();
                    ReleaseQueuedBytes(abortedBytes);

                    try
                    {
                        OnFlushed?.Invoke(0);
                    }
                    catch (Exception ex)
                    {
                        Tracing.For("Transport").Warning(this, "OnFlushed callback threw: {0}", ex);
                    }

                    break;
                }

                var total = await WriteBatchAsync(batch, ct);

                for (var i = 0; i < batch.Count; i++)
                {
                    batch[i].Dispose();
                }

                batch.Clear();
                ReleaseQueuedBytes(total);

                try
                {
                    OnFlushed?.Invoke(total);
                }
                catch (Exception ex)
                {
                    Tracing.For("Transport").Warning(this, "OnFlushed callback threw: {0}", ex);
                }
            }
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            var tornDownBytes = 0;
            for (var i = 0; i < batch.Count; i++)
            {
                tornDownBytes += batch[i].Length;
                batch[i].Dispose();
            }

            ReleaseQueuedBytes(tornDownBytes);
        }
        finally
        {
            // Single-sourced release for every item that left the channel without going through the
            // normal-write / abort / teardown-catch release above: the abort branch's `break` and a
            // cancellation-during-WaitToReadAsync both land here with items still queued (the abort
            // branch only pulled the CURRENT batch; DisposeAsync cancels the lifetime CTS before the
            // writer is completed). Accumulate their bytes and release once so _queuedBytes lands at 0.
            var leftoverBytes = 0;
            while (reader.TryRead(out var leftover))
            {
                leftoverBytes += leftover.Length;
                leftover.Dispose();
            }

            ReleaseQueuedBytes(leftoverBytes);
        }
    }

    /// <summary>Called before the send loop is drained (e.g. socket shutdown).</summary>
    protected virtual ValueTask PreDrainShutdownAsync() => default;

    /// <summary>Called after the send loop has finished (e.g. stream dispose).</summary>
    protected virtual ValueTask PostDrainShutdownAsync() => default;

    /// <remarks>Single-caller contract — see <see cref="QuiesceAsync"/>. Not safe to call concurrently.</remarks>
    public async ValueTask DisposeAsync()
    {
        var receiveCts = _receiveCts;
        await receiveCts.CancelAsync();
        await _lifetimeCts.CancelAsync();

        await PreDrainShutdownAsync();

        _completed = true;
        _channel.Writer.TryComplete();

        try
        {
            await _sendLoop;
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        await PostDrainShutdownAsync();

        receiveCts.Dispose();
        _lifetimeCts.Dispose();
    }
}
