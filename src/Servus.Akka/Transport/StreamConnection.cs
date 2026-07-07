using System.Net.Quic;

namespace Servus.Akka.Transport;

/// <summary>
/// Stream-shaped duplex connection (TLS <c>SslStream</c>, <c>QuicStream</c>, and test streams). Unlike
/// <see cref="RawSocketConnection"/> there is NO zero-byte probe — the inbound side rents a
/// <see cref="WireBuffer"/> at the current hint and awaits <c>Stream.ReadAsync</c> directly, meaning a
/// buffer IS pinned while a receive is parked. Outbound coalesces a batch of small buffers into one
/// write, otherwise writes each buffer sequentially — one flush per batch. When <c>quicAware</c> is set,
/// a graceful/aborted <see cref="QuicException"/> on read maps to EOF and the send loop skips writes once
/// the peer has closed the write side (STOP_SENDING).
/// </summary>
internal sealed class StreamConnection : DuplexConnectionBase
{
    private readonly Stream _stream;
    private readonly bool _quicAware;

    public StreamConnection(Stream stream, TransportConnectionOptions options, bool quicAware = false)
        : base(options.ReceiveBufferHint)
    {
        _stream = stream;
        _quicAware = quicAware;
    }

    internal StreamConnection(Stream stream, TransportConnectionOptions options, Task? sendLoopStartGate)
        : base(options.ReceiveBufferHint, sendLoopStartGate)
    {
        _stream = stream;
    }

    protected override async ValueTask<WireBuffer?> ReceiveDataAsync(CancellationToken ct)
    {
        // No zero-byte probe: rent at the hint and read directly. The rented buffer IS pinned while
        // the read is parked, so a quiesce that cancels this read disposes it below (clean park).
        var buffer = WireBuffer.Rent(ReceiveHint);
        try
        {
            var bytesRead = await _stream.ReadAsync(buffer.FullMemory, ct);
            if (bytesRead == 0)
            {
                buffer.Dispose();
                return null;
            }

            buffer.Length = bytesRead;
            return buffer;
        }
        catch (QuicException) when (_quicAware)
        {
            // ANY QuicException on a quic-aware read maps to graceful EOF, matching the pre-migration
            // state-machine behavior (both QUIC SMs treated every QuicException on read as
            // DisconnectReason.Graceful). Narrowing to specific QuicError codes was deliberately
            // rejected to preserve behavior during the migration.
            buffer.Dispose();
            return null;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    protected override bool ShouldAbortSendLoop() =>
        _quicAware && _stream is QuicStream { WritesClosed.IsCompleted: true };

    protected override async ValueTask<int> WriteBatchAsync(List<WireBuffer> batch, CancellationToken ct)
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

    protected override async ValueTask PostDrainShutdownAsync()
    {
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
    }
}
