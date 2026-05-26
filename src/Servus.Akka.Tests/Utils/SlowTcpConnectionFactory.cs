using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Utils;

internal sealed class SlowTcpConnectionFactory(TimeSpan delay) : ITcpConnectionFactory
{
    public async Task<ConnectionLease> EstablishAsync(TransportOptions options, CancellationToken ct)
    {
        await Task.Delay(delay, CancellationToken.None).ConfigureAwait(false);

        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        return new ConnectionLease(handle, state, cts, ConnectionInfo.None);
    }
}

internal sealed class SlowQuicConnectionFactory(TimeSpan delay) : IQuicConnectionFactory
{
    public async Task<QuicConnectionLease> EstablishAsync(QuicTransportOptions options,
        CancellationToken ct = default)
    {
        await Task.Delay(delay, CancellationToken.None).ConfigureAwait(false);

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);
        return new QuicConnectionLease(handle, options.MaxBidirectionalStreams);
    }
}

internal sealed class MockFactory : IQuicConnectionFactory
{
    private readonly bool _shouldFail;
    private readonly int _maxStreams;

    public int EstablishCount { get; private set; }

    public MockFactory(bool shouldFail = false, int maxStreams = 100)
    {
        _shouldFail = shouldFail;
        _maxStreams = maxStreams;
    }

    public Task<QuicConnectionLease> EstablishAsync(QuicTransportOptions options, CancellationToken ct = default)
    {
        EstablishCount++;
        if (_shouldFail)
        {
            return Task.FromException<QuicConnectionLease>(new IOException("Simulated failure"));
        }

        var handle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);
        return Task.FromResult(new QuicConnectionLease(handle, options.MaxBidirectionalStreams));
    }
}