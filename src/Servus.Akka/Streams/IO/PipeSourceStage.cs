using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Streams.IO;

internal sealed class PipeSourceStage : GraphStage<SourceShape<ReadOnlyMemory<byte>>>
{
    private readonly PipeReader _reader;
    private readonly Outlet<ReadOnlyMemory<byte>> _out = new("PipeReaderSource.Out");

    public PipeSourceStage(PipeReader reader)
    {
        _reader = reader;
        Shape = new SourceShape<ReadOnlyMemory<byte>>(_out);
    }

    public override SourceShape<ReadOnlyMemory<byte>> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

    private sealed record ReadCompleted(ReadResult Result);

    private sealed record ReadFailed(Exception Error);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly PipeSourceStage _stage;
        private IActorRef _stageActor = ActorRefs.Nobody;
        private bool _completing;

        public Logic(PipeSourceStage stage) : base(stage.Shape)
        {
            _stage = stage;
            SetHandler(stage._out, onPull: OnPull);
        }

        public override void PreStart()
        {
            _stageActor = GetStageActor(OnMessage).Ref;
        }

        private void OnPull()
        {
            var vt = _stage._reader.ReadAsync();

            vt.PipeTo(_stageActor,
                success: result => new ReadCompleted(result),
                failure: ex => new ReadFailed(ex));
        }

        private void OnMessage((IActorRef sender, object msg) args)
        {
            switch (args.msg)
            {
                case ReadCompleted completed:
                    ProcessReadResult(completed.Result);
                    break;

                case ReadFailed failed:
                    _stage._reader.Complete(failed.Error);
                    FailStage(failed.Error);
                    break;
            }
        }

        private void ProcessReadResult(ReadResult result)
        {
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                _stage._reader.AdvanceTo(buffer.End);
                _stage._reader.Complete();
                CompleteStage();
                return;
            }

            if (buffer.IsEmpty)
            {
                _stage._reader.AdvanceTo(buffer.Start, buffer.End);
                OnPull();
                return;
            }

            byte[] bytes;
            if (buffer.IsSingleSegment)
            {
                bytes = buffer.FirstSpan.ToArray();
            }
            else
            {
                bytes = new byte[buffer.Length];
                var offset = 0;
                foreach (var segment in buffer)
                {
                    segment.Span.CopyTo(bytes.AsSpan(offset));
                    offset += segment.Length;
                }
            }

            _stage._reader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                _completing = true;
            }

            Push(_stage._out, bytes.AsMemory());

            if (_completing)
            {
                _stage._reader.Complete();
                CompleteStage();
            }
        }

        public override void PostStop()
        {
            _stage._reader.Complete();
        }
    }
}