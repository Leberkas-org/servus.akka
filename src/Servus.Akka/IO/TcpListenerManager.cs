using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;
using Servus.Core;

namespace Servus.Akka.IO;
public record BindTcp(
    IPEndPoint EndPoint,
    int MaxFrameSize,
    IActorRef Handler,
    Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? InboundChannel = null,
    Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? OutboundChannel = null);

public record TcpClientConnected(
    EndPoint RemoteEndPoint,
    ChannelReader<(IMemoryOwner<byte> buffer, int readableBytes)> InboundReader,
    ChannelWriter<(IMemoryOwner<byte> buffer, int readableBytes)> OutboundWriter);

public record TcpDisconnected(EndPoint RemoteEndPoint);


public class TcpListenerManager : ReceiveActor
{
    private record ConnectionState(TcpListener Listener, int MaxFrameSize,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? InboundChannel = null,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? OutboundChannel = null);

    private readonly Dictionary<int, ConnectionState> _listeners = [];
    private readonly CancellationTokenSource _cts = new();

    private record NewTcpClient(TcpClient Client, IActorRef Handler,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? InboundChannel = null,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? OutboundChannel = null);

    public TcpListenerManager()
    {
        Receive<BindTcp>(msg =>
        {
            if (_listeners.TryGetValue(msg.EndPoint.Port, out var state))
            {
                state.Listener.Dispose();
            }

            var listener = new TcpListener(msg.EndPoint);
            listener.Start();

            _listeners[msg.EndPoint.Port] = new ConnectionState(listener, msg.MaxFrameSize, msg.InboundChannel, msg.OutboundChannel);

            AcceptClient(listener, msg.Handler, msg.InboundChannel, msg.OutboundChannel);
        });

        Receive<NewTcpClient>(msg =>
        {
            var port = ((IPEndPoint) msg.Client.Client.LocalEndPoint!).Port;
            var connection = _listeners[port];

            AcceptClient(connection.Listener, msg.Handler, msg.InboundChannel, msg.OutboundChannel);
            var props = Props.Create(() => new TcpClientRunner(msg.Client, connection.MaxFrameSize, msg.Handler, msg.InboundChannel, msg.OutboundChannel));
            Context.ActorOf(props, $"runner-{msg.Client.Client.RemoteEndPoint}");

            var count = Context.GetChildren().Count();
            Context.GetLogger().Debug("Current connected clients [{Count}]", Context.GetChildren().Count());
            Console.WriteLine($"Current client count: {count}");
        });
    }

    private void AcceptClient(TcpListener listener, IActorRef handler,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? inboundChannel = null,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? outboundChannel = null)
    {
        listener.AcceptTcpClientAsync(_cts.Token)
            .PipeTo(Self, success: client => new NewTcpClient(client, handler, inboundChannel, outboundChannel));
    }

    protected override void PostStop()
    {
        base.PostStop();
        _listeners.ForEach(f => f.Value.Listener.Dispose());
        _cts.Cancel();
    }
}