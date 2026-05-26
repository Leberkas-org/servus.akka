using System.Buffers;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic;

internal sealed class QuicPumpManager
{
    private readonly IActorRef _self;
    private CancellationTokenSource? _pumpsCts;
    private CancellationTokenSource? _acceptCts;

    public QuicPumpManager(IActorRef self)
    {
        _self = self;
    }

    public void StartInboundPump(StreamHandle handle, long streamId, int gen)
    {
        _pumpsCts ??= new CancellationTokenSource();
        _ = DirectStreamPumpAsync(handle, streamId, _pumpsCts.Token, _self, gen);
    }

    public void StartAcceptLoop(QuicConnectionHandle connectionHandle)
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = new CancellationTokenSource();
        _ = AcceptLoopAsync(connectionHandle, _self, _acceptCts.Token);
    }

    public void StopAll()
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = null;

        _pumpsCts?.Cancel();
        _pumpsCts?.Dispose();
        _pumpsCts = null;
    }

    private static async Task AcceptLoopAsync(
        QuicConnectionHandle handle, IActorRef self, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var result = await handle.AcceptInboundStreamAsync(ct).ConfigureAwait(false);

            if (ct.IsCancellationRequested)
            {
                if (result is not null)
                {
                    await result.Value.Stream.DisposeAsync().ConfigureAwait(false);
                }

                return;
            }

            if (result is null)
            {
                continue;
            }

            self.Tell(new InboundStreamAccepted(result.Value.Stream, result.Value.StreamId));
        }
    }

    private static async Task DirectStreamPumpAsync(StreamHandle handle, long streamId, CancellationToken ct,
        IActorRef self, int gen)
    {
        var closeReason = DisconnectReason.Graceful;
        var pool = MemoryPool<byte>.Shared;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var owner = pool.Rent(16384);
                int bytesRead;
                try
                {
                    bytesRead = await handle.ReadAsync(owner.Memory, ct).ConfigureAwait(false);
                }
                catch
                {
                    owner.Dispose();
                    throw;
                }

                if (bytesRead == 0)
                {
                    owner.Dispose();
                    break;
                }

                var tb = TransportBuffer.Rent(bytesRead);
                owner.Memory.Span[..bytesRead].CopyTo(tb.FullMemory.Span);
                tb.Length = bytesRead;
                owner.Dispose();

                self.Tell(new InboundData(tb, streamId, gen));
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            self.Tell(new InboundPumpFailed(ex, streamId));
            return;
        }

        self.Tell(new InboundComplete(closeReason, gen, streamId));
    }
}