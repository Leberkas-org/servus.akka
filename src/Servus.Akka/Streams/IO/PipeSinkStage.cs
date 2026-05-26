using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Streams.IO;

internal sealed class PipeSinkStage : GraphStageWithMaterializedValue<SinkShape<ReadOnlyMemory<byte>>, Task>
{
    private readonly PipeWriter _writer;
    private readonly Inlet<ReadOnlyMemory<byte>> _in = new("PipeWriterSink.In");

    public PipeSinkStage(PipeWriter writer)
    {
        _writer = writer;
        Shape = new SinkShape<ReadOnlyMemory<byte>>(_in);
    }

    public override SinkShape<ReadOnlyMemory<byte>> Shape { get; }

    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logic = new Logic(this, tcs);
        return new LogicAndMaterializedValue<Task>(logic, tcs.Task);
    }

    private sealed record FlushCompleted(FlushResult Result);

    private sealed record FlushFailed(Exception Error);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly PipeSinkStage _stage;
        private readonly TaskCompletionSource _tcs;
        private IActorRef _stageActor = ActorRefs.Nobody;

        public Logic(PipeSinkStage stage, TaskCompletionSource tcs) : base(stage.Shape)
        {
            _stage = stage;
            _tcs = tcs;

            SetHandler(stage._in,
                onPush: OnPush,
                onUpstreamFinish: () =>
                {
                    _stage._writer.Complete();
                    _tcs.TrySetResult();
                    CompleteStage();
                },
                onUpstreamFailure: ex =>
                {
                    _stage._writer.Complete(ex);
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

            var vt = _stage._writer.WriteAsync(chunk);

            if (vt.IsCompleted)
            {
                ProcessFlushResult(vt.Result);
                return;
            }

            _ = vt.PipeTo(_stageActor,
                success: result => new FlushCompleted(result),
                failure: ex => new FlushFailed(ex));
        }

        private void OnMessage((IActorRef sender, object msg) args)
        {
            switch (args.msg)
            {
                case FlushCompleted completed:
                    ProcessFlushResult(completed.Result);
                    break;

                case FlushFailed failed:
                    _stage._writer.Complete(failed.Error);
                    _tcs.TrySetException(failed.Error);
                    CompleteStage();
                    break;
            }
        }

        private void ProcessFlushResult(FlushResult result)
        {
            if (result.IsCompleted || result.IsCanceled)
            {
                _stage._writer.Complete();
                _tcs.TrySetResult();
                CompleteStage();
                return;
            }

            Pull(_stage._in);
        }

        public override void PostStop()
        {
            _stage._writer.CancelPendingFlush();
            _tcs.TrySetCanceled();
        }
    }
}