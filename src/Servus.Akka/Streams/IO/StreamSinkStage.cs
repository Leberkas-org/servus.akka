using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Streams.IO;

internal sealed class StreamSinkStage : GraphStageWithMaterializedValue<SinkShape<ReadOnlyMemory<byte>>, Task>
{
    private readonly Stream _stream;
    private readonly Inlet<ReadOnlyMemory<byte>> _in = new("StreamSink.In");

    public StreamSinkStage(Stream stream)
    {
        _stream = stream;
        Shape = new SinkShape<ReadOnlyMemory<byte>>(_in);
    }

    public override SinkShape<ReadOnlyMemory<byte>> Shape { get; }

    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logic = new Logic(this, tcs);
        return new LogicAndMaterializedValue<Task>(logic, tcs.Task);
    }

    private sealed record WriteCompleted;

    private sealed record WriteFailed(Exception Error);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly StreamSinkStage _stage;
        private readonly TaskCompletionSource _tcs;
        private IActorRef _stageActor = ActorRefs.Nobody;

        public Logic(StreamSinkStage stage, TaskCompletionSource tcs) : base(stage.Shape)
        {
            _stage = stage;
            _tcs = tcs;

            SetHandler(stage._in,
                onPush: OnPush,
                onUpstreamFinish: () =>
                {
                    var vt = _stage._stream.FlushAsync();

                    if (vt.IsCompleted)
                    {
                        _tcs.TrySetResult();
                        CompleteStage();
                        return;
                    }

                    vt.PipeTo(_stageActor,
                        success: () => new WriteCompleted(),
                        failure: ex => new WriteFailed(ex));
                },
                onUpstreamFailure: ex =>
                {
                    _tcs.TrySetException(ex);
                    FailStage(ex);
                });
        }

        public override void PreStart()
        {
            _stageActor = GetStageActor(OnMessage).Ref;
            Pull(_stage._in);
        }

        private void OnPush()
        {
            var chunk = Grab(_stage._in);
            if (chunk.Length == 0)
            {
                Pull(_stage._in);
                return;
            }

            var vt = _stage._stream.WriteAsync(chunk);

            if (vt.IsCompleted)
            {
                Pull(_stage._in);
                return;
            }

            vt.AsTask().PipeTo(_stageActor,
                success: () => new WriteCompleted(),
                failure: ex => new WriteFailed(ex));
        }

        private void OnMessage((IActorRef sender, object msg) args)
        {
            switch (args.msg)
            {
                case WriteCompleted:
                    if (IsClosed(_stage._in))
                    {
                        _tcs.TrySetResult();
                        CompleteStage();
                    }
                    else
                    {
                        Pull(_stage._in);
                    }
                    break;

                case WriteFailed failed:
                    _tcs.TrySetException(failed.Error);
                    FailStage(failed.Error);
                    break;
            }
        }

        public override void PostStop()
        {
            _tcs.TrySetCanceled();
        }
    }
}
