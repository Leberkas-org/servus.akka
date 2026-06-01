using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Servus.Akka.Streams;

public interface IDynamicHub<in TKey, T> where TKey : notnull
{
    Source<T, NotUsed> Source(TKey key);
}

public sealed class DynamicHub<TKey, T> : GraphStageWithMaterializedValue<SinkShape<T>, IDynamicHub<TKey, T>>
    where TKey : notnull
{
    private readonly Func<T, TKey> _keySelector;
    private readonly int _bufferSize;
    private readonly int _perConsumerBufferSize;

    private readonly Inlet<T> _in = new("DynamicHub.In");

    public override SinkShape<T> Shape { get; }

    public DynamicHub(Func<T, TKey> keySelector, int bufferSize = 256, int perConsumerBufferSize = 16)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bufferSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(perConsumerBufferSize);

        _keySelector = keySelector;
        _bufferSize = bufferSize;
        _perConsumerBufferSize = perConsumerBufferSize;
        Shape = new SinkShape<T>(_in);
    }

    public override ILogicAndMaterializedValue<IDynamicHub<TKey, T>> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var coordinatorTcs = new TaskCompletionSource<IActorRef>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var logic = new CoordinatorLogic(this, coordinatorTcs);
        var hub = new HubImpl(coordinatorTcs.Task, _perConsumerBufferSize);
        return new LogicAndMaterializedValue<IDynamicHub<TKey, T>>(logic, hub);
    }

    private sealed record Register(TKey Key, IActorRef Source);

    private sealed record Unregister(TKey Key);

    private sealed record Ack(TKey Key, int Count);

    private sealed record Deliver(T Element);

    private sealed record HubCompleted(Exception? Failure);

    private sealed class ConsumerSlot
    {
        public IActorRef? Source;
        public readonly Queue<T> HubQueue = new();
        public int Credit;
    }

    private sealed class CoordinatorLogic : GraphStageLogic
    {
        private readonly DynamicHub<TKey, T> _hub;
        private readonly TaskCompletionSource<IActorRef> _coordinatorTcs;
        private readonly Dictionary<TKey, ConsumerSlot> _slots = [];
        private int _totalBuffered;
        private bool _completing;

        public CoordinatorLogic(DynamicHub<TKey, T> hub, TaskCompletionSource<IActorRef> coordinatorTcs)
            : base(hub.Shape)
        {
            _hub = hub;
            _coordinatorTcs = coordinatorTcs;

            SetHandler(hub._in,
                onPush: OnPush,
                onUpstreamFinish: OnUpstreamFinish,
                onUpstreamFailure: OnUpstreamFailure);
        }

        public override void PreStart()
        {
            var coordinator = GetStageActor(OnMessage).Ref;
            _coordinatorTcs.SetResult(coordinator);
            Pull(_hub._in);
        }

        private void OnPush()
        {
            var element = Grab(_hub._in);

            TKey key;
            try
            {
                key = _hub._keySelector(element);
            }
            catch (Exception ex)
            {
                Fail(ex);
                return;
            }

            if (!_slots.TryGetValue(key, out var slot))
            {
                slot = new ConsumerSlot();
                _slots[key] = slot;
            }

            if (slot.Source is not null && slot is { Credit: > 0, HubQueue.Count: 0 })
            {
                slot.Credit--;
                slot.Source.Tell(new Deliver(element));
            }
            else
            {
                slot.HubQueue.Enqueue(element);
                _totalBuffered++;
            }

            AfterStateChange();
        }

        private void OnMessage((IActorRef sender, object msg) args)
        {
            switch (args.msg)
            {
                case Register(var key, var source):
                    if (!_slots.TryGetValue(key, out var rslot))
                    {
                        rslot = new ConsumerSlot();
                        _slots[key] = rslot;
                    }
                    else
                    {
                        rslot.Source?.Tell(new HubCompleted(null));
                    }

                    rslot.Source = source;
                    rslot.Credit = _hub._perConsumerBufferSize;
                    DrainSlot(rslot);
                    AfterStateChange();
                    break;

                case Ack(var key, var count):
                    if (_slots.TryGetValue(key, out var aslot))
                    {
                        aslot.Credit += count;
                        DrainSlot(aslot);
                        AfterStateChange();
                    }

                    break;

                case Unregister(var key):
                    if (_slots.Remove(key, out var removed))
                    {
                        _totalBuffered -= removed.HubQueue.Count;
                    }

                    AfterStateChange();
                    break;
            }
        }

        private void DrainSlot(ConsumerSlot slot)
        {
            while (slot.Source is not null && slot is { Credit: > 0, HubQueue.Count: > 0 })
            {
                slot.Credit--;
                _totalBuffered--;
                slot.Source.Tell(new Deliver(slot.HubQueue.Dequeue()));
            }
        }

        private void AfterStateChange()
        {
            if (_completing)
            {
                var doneKeys = new List<TKey>();
                foreach (var (key, slot) in _slots)
                {
                    if (slot.Source is null || slot.HubQueue.Count == 0)
                    {
                        slot.Source?.Tell(new HubCompleted(null));
                        doneKeys.Add(key);
                    }
                }

                foreach (var key in doneKeys)
                {
                    _slots.Remove(key);
                }

                if (_slots.Count == 0)
                {
                    CompleteStage();
                }
            }
            else if (_totalBuffered < _hub._bufferSize && !HasBeenPulled(_hub._in) && !IsClosed(_hub._in))
            {
                Pull(_hub._in);
            }
        }

        private void OnUpstreamFinish()
        {
            _completing = true;
            AfterStateChange();
        }

        private void OnUpstreamFailure(Exception ex)
        {
            Fail(ex);
        }

        private void Fail(Exception ex)
        {
            foreach (var slot in _slots.Values)
            {
                slot.Source?.Tell(new HubCompleted(ex));
            }

            _slots.Clear();
            FailStage(ex);
        }
    }

    private sealed class HubImpl(Task<IActorRef> coordinatorTask, int perConsumerBufferSize)
        : IDynamicHub<TKey, T>
    {
        public Source<T, NotUsed> Source(TKey key)
            => global::Akka.Streams.Dsl.Source.FromGraph(
                new HubSourceStage(coordinatorTask, key, perConsumerBufferSize));
    }

    private sealed class HubSourceStage : GraphStage<SourceShape<T>>
    {
        private readonly Task<IActorRef> _coordinatorTask;
        private readonly TKey _key;
        private readonly int _perConsumerBufferSize;
        private readonly Outlet<T> _out = new("DynamicHub.Source.Out");

        public override SourceShape<T> Shape { get; }

        public HubSourceStage(Task<IActorRef> coordinatorTask, TKey key, int perConsumerBufferSize)
        {
            _coordinatorTask = coordinatorTask;
            _key = key;
            _perConsumerBufferSize = perConsumerBufferSize;
            Shape = new SourceShape<T>(_out);
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
            => new SourceLogic(this);

        private sealed record CoordinatorReady(IActorRef Coordinator);

        private sealed class SourceLogic : GraphStageLogic
        {
            private readonly HubSourceStage _stage;
            private readonly Queue<T> _buffer = new();
            private IActorRef? _self;
            private IActorRef? _coordinator;
            private int _consumedSinceAck;
            private bool _completionPending;

            public SourceLogic(HubSourceStage stage) : base(stage.Shape)
            {
                _stage = stage;
                SetHandler(stage._out, onPull: OnPull);
            }

            public override void PreStart()
            {
                _self = GetStageActor(OnMessage).Ref;
                _stage._coordinatorTask.PipeTo(_self, success: c => new CoordinatorReady(c));
            }

            private void OnPull()
            {
                if (_buffer.Count > 0)
                {
                    Push(_stage._out, _buffer.Dequeue());
                    AfterConsume();
                }

                if (_completionPending && _buffer.Count == 0)
                {
                    CompleteStage();
                }
            }

            private void OnMessage((IActorRef sender, object msg) args)
            {
                switch (args.msg)
                {
                    case CoordinatorReady(var coordinator):
                        _coordinator = coordinator;
                        coordinator.Tell(new Register(_stage._key, _self!));
                        break;

                    case Deliver(var element):
                        if (IsAvailable(_stage._out) && _buffer.Count == 0)
                        {
                            Push(_stage._out, element);
                            AfterConsume();
                        }
                        else
                        {
                            _buffer.Enqueue(element);
                        }

                        break;

                    case HubCompleted(var failure):
                        if (failure is not null)
                        {
                            FailStage(failure);
                        }
                        else if (_buffer.Count == 0)
                        {
                            CompleteStage();
                        }
                        else
                        {
                            _completionPending = true;
                        }

                        break;
                }
            }

            private void AfterConsume()
            {
                _consumedSinceAck++;
                var threshold = Math.Max(1, _stage._perConsumerBufferSize / 2);
                if (_consumedSinceAck >= threshold)
                {
                    _coordinator?.Tell(new Ack(_stage._key, _consumedSinceAck));
                    _consumedSinceAck = 0;
                }
            }

            public override void PostStop()
            {
                _coordinator?.Tell(new Unregister(_stage._key));
            }
        }
    }
}