using System.Net;
using Akka.Streams;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Listener;

namespace Servus.Akka.Tests.Transport.Tcp.Listener;

public sealed class TcpServerConnectionStageSpec
{
    [Fact(Timeout = 5000)]
    public void TcpServerConnectionStage_should_have_flow_shape()
    {
        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 5000),
            new IPEndPoint(IPAddress.Loopback, 12345),
            TransportProtocol.Tcp);

        var stage = new TcpServerConnectionStage(Stream.Null, connectionInfo);

        Assert.NotNull(stage.Shape);
        Assert.IsType<FlowShape<ITransportOutbound, ITransportInbound>>(stage.Shape);
    }

    [Fact(Timeout = 5000)]
    public void TcpServerConnectionStage_shape_should_have_correct_port_names()
    {
        var connectionInfo = new ConnectionInfo(
            new IPEndPoint(IPAddress.Loopback, 5000),
            new IPEndPoint(IPAddress.Loopback, 12345),
            TransportProtocol.Tcp);

        var stage = new TcpServerConnectionStage(Stream.Null, connectionInfo);
        var shape = stage.Shape;

        Assert.Contains("TcpServerConnection", shape.Inlet.ToString());
        Assert.Contains("TcpServerConnection", shape.Outlet.ToString());
    }
}
