namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicConnectionFactory : IQuicConnectionFactory
{
    public static readonly QuicConnectionFactory Instance = new();

    public async Task<QuicConnectionLease> EstablishAsync(
        QuicTransportOptions options, CancellationToken ct = default)
    {
        var provider = new QuicClientProvider(options);
        await provider.ConnectAsync(ct).ConfigureAwait(false);

        var handle = new QuicConnectionHandle(
            openStream: async (direction, token) =>
            {
                var stream = direction == StreamDirection.Bidirectional
                    ? await provider.GetStreamAsync(token).ConfigureAwait(false)
                    : await provider.GetUnidirectionalStreamAsync(token).ConfigureAwait(false);
                var streamId = stream is System.Net.Quic.QuicStream qs ? qs.Id : -1;
                return (stream, streamId);
            },
            acceptInboundStream: async token =>
            {
                Stream stream;
                try
                {
                    stream = await provider.AcceptInboundStreamAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
                catch (Exception)
                {
                    return null;
                }
                var streamId = stream is System.Net.Quic.QuicStream qs ? qs.Id : -1;
                return (stream, streamId);
            },
            getLocalEndPoint: () => provider.LocalEndPoint,
            getRemoteEndPoint: () => provider.RemoteEndPoint,
            dispose: () => provider.DisposeAsync());

        return new QuicConnectionLease(handle, options.MaxBidirectionalStreams);
    }
}

#pragma warning restore CA1416
