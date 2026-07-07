using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// Shared base for full-duplex byte connections. Owns the inbound receive-guard (reentrancy + quiesce
/// settlement via a zero-alloc <see cref="ManualResetValueTaskSourceCore{TResult}"/>), the adaptive
/// receive-hint, the outbound <see cref="Channel{T}"/>, the send-loop skeleton, and the
/// <see cref="DisposeAsync"/> lifecycle. No <see cref="Interlocked"/> or <c>Volatile</c> — all
/// cross-thread signaling flows through <see cref="CancellationTokenSource.CancelAsync"/> and the
/// settle source's happens-before edge (Reset on the actor thread, SetResult on the I/O thread).
/// </summary>
internal abstract class DuplexConnectionBase : IDuplexConnection, IValueTaskSource<bool>
{
    private const int MaxBatchSize = 64;

    private readonly Channel<WireBuffer> _channel;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _lifetimeCts;

    private CancellationTokenSource _receiveCts;
    private ManualResetValueTaskSourceCore<bool> _settleSource;
    private bool _receiveActive;
    private int _receiveHint;
    private int _shrinkStreak;

    public Action<int>? OnFlushed { get; set; }

    protected int ReceiveHint => _receiveHint;

    protected DuplexConnectionBase(int receiveBufferHint, Task? sendLoopStartGate = null)
    {
        _receiveHint = receiveBufferHint;
        _receiveCts = new CancellationTokenSource();
        _lifetimeCts = new CancellationTokenSource();
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

    public bool TryEnqueue(WireBuffer buffer) => _channel.Writer.TryWrite(buffer);

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
                    for (var i = 0; i < batch.Count; i++)
                    {
                        batch[i].Dispose();
                    }

                    batch.Clear();

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
            for (var i = 0; i < batch.Count; i++)
            {
                batch[i].Dispose();
            }
        }
        finally
        {
            while (reader.TryRead(out var leftover))
            {
                leftover.Dispose();
            }
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
