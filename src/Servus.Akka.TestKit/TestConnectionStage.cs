using System.Collections.Concurrent;
using System.Threading.Channels;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public sealed class TestConnectionStage : GraphStage<FlowShape<ITransportOutbound, ITransportInbound>>
{
    private readonly List<OutboundHandler> _handlers;
    private readonly ActivityLog? _activityLog;
    private readonly BehaviorStack<ITransportOutbound, ITransportInbound?> _responses = new(_ => null);
    private readonly Queue<ITransportInbound> _initialInbound = new();

    private readonly Channel<ITransportInbound> _inboundChannel =
        Channel.CreateUnbounded<ITransportInbound>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly Channel<ITransportOutbound> _outboundChannel =
        Channel.CreateUnbounded<ITransportOutbound>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true
        });

    private readonly ConcurrentBag<ITransportOutbound> _receivedOutbound = [];

    private int _outboundIndex;
    private int _inboundIndex;

    public Inlet<ITransportOutbound> In { get; } = new("TestConnection.In");
    public Outlet<ITransportInbound> Out { get; } = new("TestConnection.Out");

    public override FlowShape<ITransportOutbound, ITransportInbound> Shape { get; }

    internal TestConnectionStage(List<OutboundHandler> handlers, ActivityLog? activityLog)
    {
        _handlers = handlers;
        _activityLog = activityLog;
        Shape = new FlowShape<ITransportOutbound, ITransportInbound>(In, Out);
    }

    internal void EnqueueInitial(ITransportInbound message)
        => _initialInbound.Enqueue(message);

    public void PushOnce(ITransportInbound message)
        => _inboundChannel.Writer.TryWrite(message);

    public void PushInbound(ITransportInbound message)
        => _inboundChannel.Writer.TryWrite(message);

    public async Task<ITransportOutbound> WaitForOutbound(CancellationToken ct = default)
        => await _outboundChannel.Reader.ReadAsync(ct).ConfigureAwait(false);

    public bool TryGetOutbound(out ITransportOutbound? message)
        => _outboundChannel.Reader.TryRead(out message);

    public IReadOnlyCollection<ITransportOutbound> ReceivedOutbound => _receivedOutbound;

    public void PushResponse(Func<ITransportOutbound, ITransportInbound?> handler)
        => _responses.Push(handler);

    public void PushResponseOnce(Func<ITransportOutbound, ITransportInbound?> handler)
        => _responses.PushOnce(handler);

    public void PushResponseConstant(ITransportInbound response)
        => _responses.PushConstant(response);

    public void PushResponseError(Exception exception)
        => _responses.PushError(exception);

    public DelayGate<ITransportOutbound, ITransportInbound?> PushResponseDelayed()
        => _responses.PushDelayed();

    public void PopResponse()
        => _responses.Pop();

    public static implicit operator Flow<ITransportOutbound, ITransportInbound, NotUsed>(TestConnectionStage stage)
        => Flow.FromGraph(stage);

    public Flow<ITransportOutbound, ITransportInbound, NotUsed> AsFlow()
        => Flow.FromGraph(this);

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

    private sealed class Logic : TimerGraphStageLogic, IStageContext
    {
        private readonly TestConnectionStage _stage;
        private readonly Queue<ITransportInbound> _pendingInbound = new();
        private bool _downstreamWaiting;
        private Action<ITransportInbound>? _onInboundCallback;

        public Logic(TestConnectionStage stage) : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage.In,
                onPush: () =>
                {
                    var item = Grab(stage.In);
                    var index = _stage._outboundIndex++;

                    _stage._receivedOutbound.Add(item);
                    _stage._outboundChannel.Writer.TryWrite(item);
                    _stage._activityLog?.Record(new OutboundReceived(index, item));

                    InvokeHandlers(item);

                    if (!IsClosed(stage.In))
                    {
                        Pull(stage.In);
                    }

                    TryPushNext();
                },
                onUpstreamFinish: () =>
                {
                    _stage._outboundChannel.Writer.TryComplete();
                },
                onUpstreamFailure: ex =>
                {
                    _stage._activityLog?.Record(new StageFailed(ex));
                    FailStage(ex);
                });

            SetHandler(stage.Out,
                onPull: () =>
                {
                    _downstreamWaiting = true;
                    TryPushNext();
                },
                onDownstreamFinish: _ =>
                {
                    if (!IsClosed(stage.In))
                    {
                        Cancel(stage.In);
                    }

                    _stage._outboundChannel.Writer.TryComplete();
                });
        }

        public override void PreStart()
        {
            while (_stage._initialInbound.TryDequeue(out var initial))
            {
                _pendingInbound.Enqueue(initial);
            }

            _onInboundCallback = GetAsyncCallback<ITransportInbound>(inbound =>
            {
                _pendingInbound.Enqueue(inbound);
                TryPushNext();
            });

            Pull(_stage.In);
            ScheduleInboundPoll();
        }

        public override void PostStop()
        {
            _stage._activityLog?.Record(new StageCompleted());
            _stage._outboundChannel.Writer.TryComplete();
            _stage._inboundChannel.Writer.TryComplete();
        }

        protected override void OnTimer(object timerKey)
        {
        }

        private void ScheduleInboundPoll()
        {
            var callback = _onInboundCallback!;
            var reader = _stage._inboundChannel.Reader;

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in reader.ReadAllAsync())
                    {
                        callback(item);
                    }
                }
                catch (ChannelClosedException)
                {
                }
            });
        }

        private void TryPushNext()
        {
            if (!_downstreamWaiting)
            {
                return;
            }

            if (_pendingInbound.TryDequeue(out var next))
            {
                _downstreamWaiting = false;
                Push(_stage.Out, next);
            }
        }

        private void InvokeHandlers(ITransportOutbound item)
        {
            var itemType = item.GetType();
            foreach (var handler in _stage._handlers)
            {
                if (handler.MessageType.IsAssignableFrom(itemType))
                {
                    _stage._activityLog?.Record(
                        new HandlerInvoked(itemType.Name, item));
                    handler.Invoke(item, this);
                }
            }

            var response = _stage._responses.Apply(item);
            if (response is not null)
            {
                ((IStageContext)this).Push(response);
            }
        }

        void IStageContext.Push(ITransportInbound inbound)
        {
            var index = _stage._inboundIndex++;
            _stage._activityLog?.Record(new InboundPushed(index, inbound));
            _pendingInbound.Enqueue(inbound);
            TryPushNext();
        }

        void IStageContext.Complete() => CompleteStage();

        void IStageContext.Fail(Exception ex)
        {
            _stage._activityLog?.Record(new StageFailed(ex));
            FailStage(ex);
        }

        void IStageContext.ScheduleTimer(string key, TimeSpan delay) => ScheduleOnce(key, delay);

        void IStageContext.CancelTimer(string key) => CancelTimer(key);
    }

    internal sealed class OutboundHandler(Type messageType, Action<ITransportOutbound, IStageContext> handler)
    {
        public Type MessageType { get; } = messageType;

        public void Invoke(ITransportOutbound message, IStageContext context) => handler(message, context);
    }
}