using System.Net.Sockets;
using System.Threading.Channels;

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

    private readonly Socket _socket;
    private readonly SocketAwaitable _receiver = new();
    private readonly SocketAwaitable _sender = new();
    private readonly Channel<WireBuffer> _channel;
    private readonly Task _sendLoop;
    private readonly CancellationTokenSource _lifetimeCts;

    private CancellationTokenSource _receiveCts;
    private TaskCompletionSource<ReceiveOutcome>? _receiveSettle;
    private int _receiveHint;
    private int _shrinkStreak;
    private int _receiveActive;

    public Action<int>? OnFlushed { get; set; }

    public RawSocketConnection(Socket socket, TransportConnectionOptions options)
    {
        _socket = socket;
        _receiveHint = options.ReceiveBufferHint;
        _receiveCts = new CancellationTokenSource();
        _lifetimeCts = new CancellationTokenSource();

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
        if (Interlocked.Exchange(ref _receiveActive, 1) == 1)
        {
            throw new InvalidOperationException(
                "Concurrent ReceiveAsync — the connection supports one outstanding receive.");
        }

        var settle = new TaskCompletionSource<ReceiveOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        _receiveSettle = settle;
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
            Volatile.Write(ref _receiveActive, 0);
            settle.TrySetResult(outcome);
        }
    }

    public bool TryEnqueue(WireBuffer buffer) => _channel.Writer.TryWrite(buffer);

    public async ValueTask<bool> QuiesceAsync()
    {
        // Only await settlement if a receive is genuinely in flight; otherwise the stored settle task
        // belongs to a prior (already consumed) receive and would report a stale outcome.
        var settle = Volatile.Read(ref _receiveActive) == 1 ? _receiveSettle : null;

        await _receiveCts.CancelAsync();

        var clean = true;
        if (settle is not null)
        {
            var outcome = await settle.Task;
            clean = outcome == ReceiveOutcome.Cancelled;
        }

        // A cancelled CTS is single-use; swap in a fresh one so the next ReceiveAsync works after a
        // clean quiesce. On the not-clean path the caller disposes us anyway; the swap is harmless.
        _receiveCts.Dispose();
        _receiveCts = new CancellationTokenSource();
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

                var total = batch.Count == 1
                    ? await SendSingleAsync(batch[0])
                    : await _sender.SendManyAsync(_socket, batch);

                for (var i = 0; i < batch.Count; i++)
                {
                    batch[i].Dispose();
                }

                batch.Clear();
                OnFlushed?.Invoke(total);
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

    public async ValueTask DisposeAsync()
    {
        await _receiveCts.CancelAsync();
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

        _receiveCts.Dispose();
        _lifetimeCts.Dispose();
    }
}
