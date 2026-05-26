using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed class TcpServerConnectionStage : GraphStage<FlowShape<ITransportOutbound, ITransportInbound>>
{
    private readonly Stream _stream;
    private readonly ConnectionInfo _connectionInfo;
    private readonly SslStream? _sslStream;
    private readonly bool _allowDelayedNegotiation;

    private readonly Inlet<ITransportOutbound> _in = new("TcpServerConnection.In");
    private readonly Outlet<ITransportInbound> _out = new("TcpServerConnection.Out");

    public override FlowShape<ITransportOutbound, ITransportInbound> Shape { get; }

    public TcpServerConnectionStage(
        Stream stream,
        ConnectionInfo connectionInfo,
        SslStream? sslStream = null,
        bool allowDelayedNegotiation = false)
    {
        _stream = stream;
        _connectionInfo = connectionInfo;
        _sslStream = sslStream;
        _allowDelayedNegotiation = allowDelayedNegotiation;
        Shape = new FlowShape<ITransportOutbound, ITransportInbound>(_in, _out);
    }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        => new Logic(this);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : TimerGraphStageLogic, ITransportOperations
    {
        private readonly TcpServerConnectionStage _stage;
        private readonly Queue<ITransportInbound> _pendingReads = new();
        private TcpServerStateMachine _sm = null!;

        public Logic(TcpServerConnectionStage stage) : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage._in,
                onPush: () => _sm.HandlePush(Grab(stage._in)),
                onUpstreamFinish: () => _sm.HandleUpstreamFinish());

            SetHandler(stage._out,
                onPull: () =>
                {
                    if (_pendingReads.TryDequeue(out var item))
                    {
                        Push(_stage._out, item);
                    }
                },
                onDownstreamFinish: _ =>
                {
                    _sm.HandleDownstreamFinish();
                    CompleteStage();
                });
        }

        public override void PreStart()
        {
            var stageActor = GetStageActor(OnReceive);
            var state = new ClientState(_stage._stream);
            _sm = new TcpServerStateMachine(
                this, stageActor.Ref, state, _stage._connectionInfo,
                _stage._sslStream, _stage._allowDelayedNegotiation);
            _sm.Start();
            Pull(_stage._in);
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            if (args.message is ITcpTransportEvent evt)
            {
                _sm.Dispatch(evt);
            }
        }

        protected override void OnTimer(object timerKey) { }

        public override void PostStop() => _sm.PostStop();

        void ITransportOperations.OnPushInbound(ITransportInbound item)
        {
            if (IsAvailable(_stage._out))
            {
                Push(_stage._out, item);
            }
            else
            {
                _pendingReads.Enqueue(item);
            }
        }

        void ITransportOperations.OnSignalPullOutbound()
        {
            if (!IsClosed(_stage._in) && !HasBeenPulled(_stage._in))
            {
                Pull(_stage._in);
            }
        }

        void ITransportOperations.OnCompleteStage() => CompleteStage();

        void ITransportOperations.OnScheduleTimer(string key, TimeSpan delay)
            => ScheduleOnce(key, delay);

        void ITransportOperations.OnCancelTimer(string key) => CancelTimer(key);

        ILoggingAdapter ITransportOperations.Log => Log;
    }
}
