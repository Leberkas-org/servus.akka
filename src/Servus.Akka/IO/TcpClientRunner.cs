using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
using Akka.Actor;

namespace Servus.Akka.IO;

public class TcpClientRunner : ReceiveActor
{
    private readonly TcpClient _client;
    private readonly IActorRef _handler;
    private readonly TcpClientState _state;
    private readonly CancellationTokenSource _cts = new();
    private readonly IActorRef _selfClosure;

    public TcpClientRunner(TcpClient client, int maxFrameSize, IActorRef handler,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? inboundChannel = null,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? outboundChannel = null)
    {
        _client = client;
        _handler = handler;

        _state = new TcpClientState(maxFrameSize, _client.GetStream(), inboundChannel, outboundChannel);

        _selfClosure = Context.Self;

        Receive<CloseConnection>(_ =>
        {
            _cts.Cancel();
            _handler.Tell(new TcpDisconnected(_client.Client.RemoteEndPoint!));
            Context.Self.Tell(PoisonPill.Instance);
        });
    }

    protected override void PreStart()
    {
        base.PreStart();

        _handler.Tell(new TcpClientConnected(_client.Client.RemoteEndPoint!, _state.InboundReader, _state.OutboundWriter));

        _ = TcpClientByteMover.MoveStreamToPipe(_state, _selfClosure, _cts.Token);
        _ = TcpClientByteMover.MovePipeToChannel(_state, _selfClosure, _cts.Token);
        _ = TcpClientByteMover.MoveChannelToStream(_state, _selfClosure, _cts.Token);
    }
}
