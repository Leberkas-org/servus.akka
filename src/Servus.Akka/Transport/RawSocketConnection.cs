using System.Net.Sockets;
using System.Threading.Channels;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// Plaintext-TCP duplex connection. Inbound is probe-gated rent-and-receive: a zero-byte probe
/// (the only cancellation point) parks idle connections without pinning a buffer, then the data
/// receive rents a <see cref="WireBuffer"/> and reads into it. Outbound is a single-reader unbounded
/// channel of owned buffers drained by a send loop that coalesces batches into vectored sends and
/// disposes each buffer after it is fully written. <see cref="QuiesceAsync"/> parks the inbound side
/// without tearing the socket down so the connection can be reused.
/// </summary>
internal sealed class RawSocketConnection : IDuplexConnection
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

    private readonly Socket _socket;
    private readonly SocketAwaitable _receiver = new();
    private readonly SocketAwaitable _sender = new();
    private readonly Channel<WireBuffer> _channel;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _lifetimeCts;
    private readonly Task? _sendLoopStartGate;

    private CancellationTokenSource _receiveCts;
    private ReceiveSession? _receiveSession;
    private int _receiveHint;
    private int _shrinkStreak;
    private long _vectoredSendCount;

    public Action<int>? OnFlushed { get; set; }

    /// <summary>
    /// TEST-ONLY. Number of vectored (multi-buffer) send batches drained by the loop. Lets a test assert
    /// the coalescing path actually ran rather than the batch degenerating into single sends.
    /// </summary>
    internal long VectoredSendCount => Interlocked.Read(ref _vectoredSendCount);

    /// <summary>
    /// TEST-ONLY. Caps bytes per underlying socket send on the send loop's sender so partial-send
    /// remainder math is exercised deterministically. Null (default) leaves the hot path untouched.
    /// </summary>
    internal int? MaxBytesPerSendForTest
    {
        set => _sender.MaxBytesPerSendForTest = value;
    }

    public RawSocketConnection(Socket socket, TransportConnectionOptions options)
        : this(socket, options, sendLoopStartGate: null)
    {
    }

    /// <summary>
    /// TEST-ONLY constructor. <paramref name="sendLoopStartGate"/>, when non-null, is awaited once by the
    /// send loop before its first drain, so a test can enqueue N buffers while the loop is parked and
    /// guarantee they coalesce into a single vectored send.
    /// </summary>
    internal RawSocketConnection(Socket socket, TransportConnectionOptions options, Task? sendLoopStartGate)
    {
        _socket = socket;
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
            // Zero-byte probe: the only cancellation point. It pins no buffer while idle, so an idle
            // connection can be quiesced without losing data.
            await _socket.ReceiveAsync(Memory<byte>.Empty, SocketFlags.None, token);

            var buffer = WireBuffer.Rent(_receiveHint);
            try
            {
                // After a successful probe the bytes are already in the kernel buffer, so this usually
                // completes synchronously — preserving the state machines' sync fast-path.
                var bytesRead = await _receiver.ReceiveAsync(_socket, buffer.FullMemory);
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
            catch
            {
                buffer.Dispose();
                throw;
            }
        }
        catch (OperationCanceledException)
        {
            outcome = ReceiveOutcome.Cancelled;
            throw;
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
            // coalesce into one vectored send. Null in production.
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

                int total;
                if (batch.Count == 1)
                {
                    total = await SendSingleAsync(batch[0]);
                }
                else
                {
                    Interlocked.Increment(ref _vectoredSendCount);
                    total = await _sender.SendManyAsync(_socket, batch);
                }

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

    private async ValueTask<int> SendSingleAsync(WireBuffer buffer)
    {
        var memory = buffer.Memory;
        var offset = 0;

        // SAEA does not guarantee a full transfer — re-issue the unsent tail until the whole buffer
        // is written.
        while (offset < memory.Length)
        {
            var sent = await _sender.SendAsync(_socket, memory[offset..]);
            if (sent == 0)
            {
                throw new IOException("Socket send returned 0 bytes.");
            }

            offset += sent;
        }

        return memory.Length;
    }

    /// <remarks>Single-caller contract — see <see cref="QuiesceAsync"/>. Not safe to call concurrently.</remarks>
    public async ValueTask DisposeAsync()
    {
        var receiveCts = _receiveCts;
        await receiveCts.CancelAsync();
        await _lifetimeCts.CancelAsync();

        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        _socket.Close();

        _channel.Writer.TryComplete();

        try
        {
            await _sendLoop;
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        receiveCts.Dispose();
        _lifetimeCts.Dispose();
    }
}
