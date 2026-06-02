using System.Buffers;
using Akka.Actor;

namespace Servus.Akka.Transport.Quic;

internal sealed class QuicPumpManager(IActorRef self)
{
    private CancellationTokenSource? _pumpsCts;
    private CancellationTokenSource? _acceptCts;

    public void StartInboundPump(StreamHandle handle, long streamId, int gen)
    {
        _pumpsCts ??= new CancellationTokenSource();
        _ = DirectStreamPumpAsync(handle, streamId, _pumpsCts.Token, self, gen);
    }

    public void StartAcceptLoop(QuicConnectionHandle connectionHandle)
    {
        _acceptCts?.Cancel();
        _acceptCts?.Dispose();
        _acceptCts = new CancellationTokenSource();
        _ = AcceptLoopAsync(connectionHandle, self, _acceptCts.Token);
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
        const int minBufferSize = 4 * 1024;
        const int maxBufferSize = 64 * 1024;

        var closeReason = DisconnectReason.Graceful;
        var bufferSize = minBufferSize;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var tb = TransportBuffer.Rent(bufferSize);
                int bytesRead;
                try
                {
                    bytesRead = await handle.ReadAsync(tb.FullMemory, ct).ConfigureAwait(false);
                }
                catch
                {
                    tb.Dispose();
                    throw;
                }

                if (bytesRead == 0)
                {
                    tb.Dispose();
                    break;
                }

                tb.Length = bytesRead;
                self.Tell(new InboundData(tb, streamId, gen));

                if (bytesRead >= bufferSize * 3 / 4)
                {
                    bufferSize = Math.Min(bufferSize * 2, maxBufferSize);
                }
                else if (bytesRead <= bufferSize / 4 && bufferSize > minBufferSize)
                {
                    bufferSize = Math.Max(bufferSize / 2, minBufferSize);
                }
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