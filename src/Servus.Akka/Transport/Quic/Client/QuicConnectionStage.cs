using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicConnectionStage : GraphStage<FlowShape<List<ITransportOutbound>, ITransportInbound>>
{
    private readonly IActorRef _connectionManager;

    private readonly Inlet<List<ITransportOutbound>> _in = new("QuicConnection.In");
    private readonly Outlet<ITransportInbound> _out = new("QuicConnection.Out");

    public override FlowShape<List<ITransportOutbound>, ITransportInbound> Shape { get; }

    public QuicConnectionStage(IActorRef connectionManager)
    {
        _connectionManager = connectionManager;
        Shape = new FlowShape<List<ITransportOutbound>, ITransportInbound>(_in, _out);
    }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        => new Logic(this);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : TimerGraphStageLogic, ITransportOperations
    {
        private readonly QuicConnectionStage _stage;
        private readonly Queue<ITransportInbound> _pendingReads = new();
        private QuicTransportStateMachine _sm = null!;

        public Logic(QuicConnectionStage stage) : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage._in,
                onPush: () =>
                {
                    var batch = Grab(stage._in);
                    foreach (var item in batch)
                    {
                        _sm.HandlePush(item);
                    }
                },
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
            _sm = new QuicTransportStateMachine(this, _stage._connectionManager, stageActor.Ref);
            Pull(_stage._in);
        }

        private void OnReceive((IActorRef sender, object message) args)
        {
            if (args.message is IQuicTransportEvent evt)
            {
                _sm.Dispatch(evt);
            }
        }

        protected override void OnTimer(object timerKey)
            => _sm.OnTimer(timerKey as string);

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

        void ITransportOperations.OnCompleteStage()
            => CompleteStage();

        void ITransportOperations.OnScheduleTimer(string key, TimeSpan delay)
            => ScheduleOnce(key, delay);

        void ITransportOperations.OnCancelTimer(string key)
            => CancelTimer(key);

        ILoggingAdapter ITransportOperations.Log => Log;
    }
}