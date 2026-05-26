using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Quic.Listener;

internal sealed class QuicServerConnectionStage : GraphStage<FlowShape<ITransportOutbound, ITransportInbound>>
{
    private readonly QuicConnectionHandle _connectionHandle;
    private readonly ConnectionInfo _connectionInfo;

    private readonly Inlet<ITransportOutbound> _in = new("QuicServerConnection.In");
    private readonly Outlet<ITransportInbound> _out = new("QuicServerConnection.Out");

    public override FlowShape<ITransportOutbound, ITransportInbound> Shape { get; }

    public QuicServerConnectionStage(QuicConnectionHandle connectionHandle, ConnectionInfo connectionInfo)
    {
        _connectionHandle = connectionHandle;
        _connectionInfo = connectionInfo;
        Shape = new FlowShape<ITransportOutbound, ITransportInbound>(_in, _out);
    }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        => new Logic(this);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : TimerGraphStageLogic, ITransportOperations
    {
        private readonly QuicServerConnectionStage _stage;
        private readonly Queue<ITransportInbound> _pendingReads = new();
        private QuicServerStateMachine _sm = null!;

        public Logic(QuicServerConnectionStage stage) : base(stage.Shape)
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
            _sm = new QuicServerStateMachine(
                this,
                stageActor.Ref,
                _stage._connectionHandle,
                _stage._connectionInfo);
            _sm.Start();
            Pull(_stage._in);
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            if (args.message is IQuicTransportEvent evt)
            {
                _sm.Dispatch(evt);
            }
        }

        protected override void OnTimer(object timerKey) => _sm.OnTimer(timerKey as string);

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
