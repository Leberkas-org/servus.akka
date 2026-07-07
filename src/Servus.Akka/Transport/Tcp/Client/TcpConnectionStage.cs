using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Tcp.Client;

internal sealed class TcpConnectionStage : GraphStage<FlowShape<ITransportOutbound, ITransportInbound>>
{
    private readonly IActorRef _connectionManager;
    private readonly IPoolingStrategy _poolingStrategy;

    private readonly Inlet<ITransportOutbound> _in = new("TcpConnection.In");
    private readonly Outlet<ITransportInbound> _out = new("TcpConnection.Out");

    public override FlowShape<ITransportOutbound, ITransportInbound> Shape { get; }

    public TcpConnectionStage(IActorRef connectionManager, IPoolingStrategy poolingStrategy)
    {
        _connectionManager = connectionManager;
        _poolingStrategy = poolingStrategy;
        Shape = new FlowShape<ITransportOutbound, ITransportInbound>(_in, _out);
    }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : TimerGraphStageLogic, IConnectionOperations
    {
        private readonly TcpConnectionStage _stage;
        private readonly Queue<ITransportInbound> _pendingReads = new();
        private TcpConnectionStateMachine _sm = null!;
        private bool _readRequested;

        public Logic(TcpConnectionStage stage) : base(stage.Shape)
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
                        return;
                    }

                    if (!_readRequested)
                    {
                        _readRequested = true;
                        _sm.RequestRead();
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
            _sm = new TcpConnectionStateMachine(
                this,
                _stage._connectionManager,
                _stage._poolingStrategy,
                stageActor.Ref);
            Pull(_stage._in);
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            if (args.message is ITcpTransportEvent evt)
            {
                _sm.Dispatch(evt);
            }
        }

        protected override void OnTimer(object timerKey) => _sm.OnTimer(timerKey as string);

        public override void PostStop() => _sm.PostStop();

        bool IConnectionOperations.OnPushInbound(ITransportInbound item)
        {
            _readRequested = false;

            if (IsAvailable(_stage._out))
            {
                Push(_stage._out, item);
                return true;
            }

            _pendingReads.Enqueue(item);
            return false;
        }

        void IConnectionOperations.OnSignalPullOutbound()
        {
            if (!IsClosed(_stage._in) && !HasBeenPulled(_stage._in))
            {
                Pull(_stage._in);
            }
        }

        void IConnectionOperations.OnCompleteStage() => CompleteStage();

        void IConnectionOperations.OnScheduleTimer(string key, TimeSpan delay) => ScheduleOnce(key, delay);

        void IConnectionOperations.OnCancelTimer(string key) => CancelTimer(key);
    }
}