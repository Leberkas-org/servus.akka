using System.IO;
using System.Net.Quic;
using System.Threading.Channels;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// Stream-shaped duplex connection (TLS <c>SslStream</c>, <c>QuicStream</c>, and test streams). Unlike
/// <see cref="RawSocketConnection"/> there is NO zero-byte probe — plain/test streams return 0 on empty
/// reads instead of parking — so the inbound side rents a <see cref="WireBuffer"/> at the current hint
/// and awaits <c>Stream.ReadAsync</c> directly, meaning a buffer IS pinned while a receive is parked.
/// A quiesce that cancels a pending read therefore disposes the rented buffer inside the receive path and
/// counts as clean; data winning the race flows to the awaiter (buffer ownership transfers to it).
/// Outbound is a single-reader unbounded channel of owned buffers drained by a send loop that coalesces a
/// batch of small buffers into one write, otherwise writes each buffer sequentially — one flush per batch.
/// When <c>quicAware</c> is set, a graceful/aborted <see cref="QuicException"/> on read maps to EOF and
/// the send loop skips writes once the peer has closed the write side (STOP_SENDING).
/// </summary>
internal sealed class StreamConnection : IDuplexConnection
{
    private enum ReceiveOutcome
    {
        Faulted,
        Cancelled,
        Data,
        Eof,
    }

    /// <summary>
    /// One atomically-published unit of receive state: the presence of the reference is itself the
    /// "receive active" flag, and it carries that receive's terminal settlement. Folding both into a
    /// single reference swapped with <see cref="Interlocked"/> means <see cref="QuiesceAsync"/> can
    /// never observe an active receive paired with a STALE settle from a prior receive.
    /// </summary>
    private sealed class ReceiveSession
    {
        public readonly TaskCompletionSource<ReceiveOutcome> Settle =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly Stream _stream;
    private readonly bool _quicAware;
    private readonly Channel<WireBuffer> _channel;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _lifetimeCts;
    private readonly Task? _sendLoopStartGate;

    private CancellationTokenSource _receiveCts;
    private ReceiveSession? _receiveSession;
    private int _receiveHint;
    private int _shrinkStreak;
    private long _coalescedWriteCount;

    public Action<int>? OnFlushed { get; set; }

    /// <summary>
    /// TEST-ONLY. Number of coalesced (multi-buffer merged into one) writes issued by the send loop. Lets
    /// a test assert the coalescing path ran rather than the batch degenerating into sequential writes.
    /// </summary>
    internal long CoalescedWriteCount => Interlocked.Read(ref _coalescedWriteCount);

    public StreamConnection(Stream stream, TransportConnectionOptions options, bool quicAware = false)
        : this(stream, options, quicAware, sendLoopStartGate: null)
    {
    }

    /// <summary>
    /// TEST-ONLY constructor. <paramref name="sendLoopStartGate"/>, when non-null, is awaited once by the
    /// send loop before its first drain, so a test can enqueue N buffers while the loop is parked and
    /// guarantee they coalesce into a single write.
    /// </summary>
    internal StreamConnection(Stream stream, TransportConnectionOptions options, Task? sendLoopStartGate)
        : this(stream, options, quicAware: false, sendLoopStartGate)
    {
    }

    private StreamConnection(
        Stream stream,
        TransportConnectionOptions options,
        bool quicAware,
        Task? sendLoopStartGate)
    {
        _stream = stream;
        _quicAware = quicAware;
        _receiveHint = options.ReceiveBufferHint;
        _receiveCts = new CancellationTokenSource();
        _lifetimeCts = new CancellationTokenSource();
        _sendLoopStartGate = sendLoopStartGate;

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
        _sendLoop = Task.Run(() => RunSendLoopAsync(_channel.Reader, ct));
    }

    public async ValueTask<WireBuffer?> ReceiveAsync()
    {
        // Publishing the session IS the reentrancy guard: a session already present means a receive is in
        // flight. One atomic swap installs both the "active" flag and this receive's settlement together,
        // so QuiesceAsync always reads a self-consistent pair.
        var session = new ReceiveSession();
        if (Interlocked.CompareExchange(ref _receiveSession, session, null) is not null)
        {
            throw new InvalidOperationException(
                "Concurrent ReceiveAsync — the connection supports one outstanding receive.");
        }

        var token = _receiveCts.Token;
        var outcome = ReceiveOutcome.Faulted;

        try
        {
            // No zero-byte probe: rent at the hint and read directly. The rented buffer IS pinned while
            // the read is parked, so a quiesce that cancels this read disposes it below (clean park).
            var buffer = WireBuffer.Rent(_receiveHint);
            try
            {
                var bytesRead = await _stream.ReadAsync(buffer.FullMemory, token);
                if (bytesRead == 0)
                {
                    buffer.Dispose();
                    outcome = ReceiveOutcome.Eof;
                    return null;
                }

                buffer.Length = bytesRead;
                AdaptiveHint.Adapt(bytesRead, ref _receiveHint, ref _shrinkStreak);
                outcome = ReceiveOutcome.Data;
                return buffer;
            }
            catch (OperationCanceledException)
            {
                // Quiesce/dispose cancelled the parked read: dispose the pinned buffer and settle clean.
                buffer.Dispose();
                outcome = ReceiveOutcome.Cancelled;
                throw;
            }
            catch (QuicException) when (_quicAware)
            {
                // ANY QuicException on a quic-aware read maps to graceful EOF, matching the pre-migration
                // state-machine behavior (both QUIC SMs treated every QuicException on read as
                // DisconnectReason.Graceful — peer FIN/STOP_SENDING/RST_STREAM). Narrowing to specific
                // QuicError codes was deliberately rejected to preserve behavior during the migration.
                buffer.Dispose();
                outcome = ReceiveOutcome.Eof;
                return null;
            }
            catch
            {
                buffer.Dispose();
                throw;
            }
        }
        finally
        {
            // Clear the active flag before settling so a QuiesceAsync awaiter that wakes on the settle can
            // start the next receive; it holds its own reference to this session, so the clear is safe.
            Volatile.Write(ref _receiveSession, null);
            session.Settle.TrySetResult(outcome);
        }
    }

    public bool TryEnqueue(WireBuffer buffer) => _channel.Writer.TryWrite(buffer);

    /// <remarks>
    /// Single-caller contract: <see cref="QuiesceAsync"/> and <see cref="DisposeAsync"/> are never invoked
    /// concurrently with each other or with themselves. The receive-CTS swap below still uses an atomic
    /// exchange so the fresh token is safely published to a subsequent <see cref="ReceiveAsync"/> and the
    /// old CTS is disposed exactly once.
    /// </remarks>
    public async ValueTask<bool> QuiesceAsync()
    {
        // One read of the session gives an atomically-consistent (active + settle) pair: either there is a
        // receive in flight and we own its settlement, or there is none. No stale-outcome window.
        var session = Volatile.Read(ref _receiveSession);
        var current = _receiveCts;

        await current.CancelAsync();

        var clean = true;
        if (session is not null)
        {
            // A cancelled read settles Cancelled (buffer disposed here) = clean; data/EOF/fault winning the
            // race settles otherwise = not clean, and any produced buffer flowed to the receive's awaiter.
            var outcome = await session.Settle.Task;
            clean = outcome == ReceiveOutcome.Cancelled;
        }

        // A cancelled CTS is single-use; swap in a fresh one so the next ReceiveAsync works after a clean
        // quiesce. On the not-clean path the caller disposes us anyway; the swap is harmless.
        Interlocked.Exchange(ref _receiveCts, new CancellationTokenSource());
        current.Dispose();
        return clean;
    }

    public async Task CompleteAndDrainOutputAsync()
    {
        _channel.Writer.TryComplete();
        await _sendLoop;
    }

    private async Task RunSendLoopAsync(ChannelReader<WireBuffer> reader, CancellationToken ct)
    {
        var batch = new List<WireBuffer>(64);

        try
        {
            // TEST-ONLY park: lets a test enqueue a whole batch before the first drain so the buffers
            // coalesce into one write. Null in production.
            if (_sendLoopStartGate is not null)
            {
                await _sendLoopStartGate;
            }

            while (await reader.WaitToReadAsync(ct))
            {
                batch.Clear();
                while (batch.Count < 64 && reader.TryRead(out var item))
                {
                    batch.Add(item);
                }

                if (batch.Count == 0)
                {
                    continue;
                }

                // QUIC: once the peer has closed our write side (STOP_SENDING), any WriteAsync throws
                // QuicException(StreamAborted). Report 0 flushed for the skipped bytes and STOP the send
                // loop entirely — the outbound side is dead — matching the old RunQuicSendLoop break. The
                // finally drains and disposes anything still queued.
                if (_quicAware && _stream is QuicStream q && q.WritesClosed.IsCompleted)
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

                // A consumer callback must never fault the send loop — swallow and trace so one bad
                // OnFlushed handler cannot tear down the connection's outbound side.
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
            // Drain-and-dispose anything still queued so no buffer leaks on teardown or completion.
            while (reader.TryRead(out var leftover))
            {
                leftover.Dispose();
            }
        }
    }

    private async ValueTask<int> WriteBatchAsync(List<WireBuffer> batch, CancellationToken ct)
    {
        var total = 0;
        var allSmall = batch.Count > 1;
        for (var i = 0; i < batch.Count; i++)
        {
            total += batch[i].Length;
            if (batch[i].Length >= 4 * 1024)
            {
                allSmall = false;
            }
        }

        if (batch.Count == 1)
        {
            await _stream.WriteAsync(batch[0].Memory, ct);
        }
        else if (allSmall)
        {
            // Coalesce many small buffers into one rented buffer + one write so TLS emits a single record
            // and QUIC a single STREAM frame instead of N. Rent from the cross-thread WireBuffer pool.
            Interlocked.Increment(ref _coalescedWriteCount);
            using var owner = WireBuffer.Rent(total);
            var mem = owner.FullMemory[..total];
            var offset = 0;
            for (var i = 0; i < batch.Count; i++)
            {
                batch[i].Span.CopyTo(mem.Span[offset..]);
                offset += batch[i].Length;
            }

            await _stream.WriteAsync(mem, ct);
        }
        else
        {
            for (var i = 0; i < batch.Count; i++)
            {
                await _stream.WriteAsync(batch[i].Memory, ct);
            }
        }

        // Exactly one flush per drained batch, regardless of how many writes it took.
        await _stream.FlushAsync(ct);
        return total;
    }

    /// <remarks>Single-caller contract — see <see cref="QuiesceAsync"/>. Not safe to call concurrently.</remarks>
    public async ValueTask DisposeAsync()
    {
        var receiveCts = _receiveCts;
        await receiveCts.CancelAsync();
        await _lifetimeCts.CancelAsync();

        _channel.Writer.TryComplete();

        try
        {
            await _sendLoop;
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        // Dispose the stream only after the send loop has finished so no write races the disposal. The
        // cancelled receive-CTS already unblocked any parked ReadAsync.
        try
        {
            await _stream.DisposeAsync();
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        receiveCts.Dispose();
        _lifetimeCts.Dispose();
    }
}
