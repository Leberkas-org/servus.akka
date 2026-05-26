using System.Buffers;
using Akka.Actor;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Transport.Tcp;

internal sealed class TcpPumpManager
{
    private readonly IActorRef _self;
    private CancellationTokenSource? _pumpsCts;

    public TcpPumpManager(IActorRef self)
    {
        _self = self;
    }

    public void StartPumps(ClientState state, int gen)
    {
        _pumpsCts?.Cancel();
        _pumpsCts?.Dispose();
        _pumpsCts = new CancellationTokenSource();

        var ct = _pumpsCts.Token;

        _ = RunInboundPump(state, gen, ct);
        _ = ClientByteMover.MoveChannelToStream(state, () =>
        {
            _self.Tell(new OutboundWriteDone(gen));
        }, ct);
    }

    public void StopPumps()
    {
        _pumpsCts?.Cancel();
        _pumpsCts?.Dispose();
        _pumpsCts = null;
    }

    private async Task RunInboundPump(ClientState state, int gen, CancellationToken ct)
    {
        _ = ClientByteMover.MoveStreamToChannel(state, () => { }, ct);

        var closeKind = DisconnectReason.Graceful;
        try
        {
            while (await state.InboundReader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                var batch = ArrayPool<ITransportInbound>.Shared.Rent(32);
                var count = 0;

                while (count < batch.Length && state.InboundReader.TryRead(out var buf))
                {
                    batch[count++] = new TransportData(buf);
                }

                if (count > 0)
                {
                    _self.Tell(new InboundBatch(batch, count, gen));
                }
                else
                {
                    ArrayPool<ITransportInbound>.Shared.Return(batch);
                }
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            _self.Tell(new InboundPumpFailed(ex));
            return;
        }

        _self.Tell(new InboundComplete(closeKind, gen));
    }
}
