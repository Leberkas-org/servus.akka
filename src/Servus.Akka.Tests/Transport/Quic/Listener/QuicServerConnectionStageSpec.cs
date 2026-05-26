using System.Net;
using Akka.Streams;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Listener;

namespace Servus.Akka.Tests.Transport.Quic.Listener;

public sealed class QuicServerConnectionStageSpec
{
    [Fact(Timeout = 5000)]
    public void QuicServerConnectionStage_should_have_flow_shape()
    {
        var connectionHandle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult<(Stream, long)>((Stream.Null, 1)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 5000),
            getRemoteEndPoint: () => null,
            dispose: () => default);

        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 5000),
            new IPEndPoint(IPAddress.Loopback, 12345),
            TransportProtocol.None);

        var stage = new QuicServerConnectionStage(connectionHandle, connectionInfo);

        Assert.NotNull(stage.Shape);
        Assert.IsType<FlowShape<ITransportOutbound, ITransportInbound>>(stage.Shape);
    }

    [Fact(Timeout = 5000)]
    public void QuicServerConnectionStage_shape_should_have_correct_port_names()
    {
        var connectionHandle = new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult<(Stream, long)>((Stream.Null, 1)),
            acceptInboundStream: async ct => { await Task.Delay(Timeout.Infinite, ct); return null; },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 5000),
            getRemoteEndPoint: () => null,
            dispose: () => default);

        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 5000),
            new IPEndPoint(IPAddress.Loopback, 12345),
            TransportProtocol.None);

        var stage = new QuicServerConnectionStage(connectionHandle, connectionInfo);
        var shape = stage.Shape;

        Assert.Contains("QuicServerConnection", shape.Inlet.ToString());
        Assert.Contains("QuicServerConnection", shape.Outlet.ToString());
    }
}
