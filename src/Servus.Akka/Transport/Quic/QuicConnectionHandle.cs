using System.Net;

namespace Servus.Akka.Transport.Quic;

internal sealed class QuicConnectionHandle : IAsyncDisposable
{
    private readonly Func<StreamDirection, CancellationToken, Task<(Stream, long)>> _openStream;
    private readonly Func<CancellationToken, Task<(Stream, long)?>> _acceptInboundStream;
    private readonly Func<ValueTask> _dispose;
    private readonly Func<EndPoint?> _getLocalEndPoint;
    private readonly Func<EndPoint?> _getRemoteEndPoint;

    internal QuicConnectionHandle(
        Func<StreamDirection, CancellationToken, Task<(Stream, long)>> openStream,
        Func<CancellationToken, Task<(Stream, long)?>> acceptInboundStream,
        Func<EndPoint?> getLocalEndPoint,
        Func<EndPoint?> getRemoteEndPoint,
        Func<ValueTask> dispose)
    {
        _openStream = openStream;
        _acceptInboundStream = acceptInboundStream;
        _getLocalEndPoint = getLocalEndPoint;
        _getRemoteEndPoint = getRemoteEndPoint;
        _dispose = dispose;
    }

    public Task<(Stream Stream, long StreamId)> OpenStreamAsync(
        StreamDirection direction, CancellationToken ct = default)
        => _openStream(direction, ct);

    public Task<(Stream Stream, long StreamId)?> AcceptInboundStreamAsync(
        CancellationToken ct = default)
        => _acceptInboundStream(ct);

    public EndPoint? LocalEndPoint() => _getLocalEndPoint();

    public EndPoint? RemoteEndPoint() => _getRemoteEndPoint();

    public ValueTask DisposeAsync() => _dispose();
}