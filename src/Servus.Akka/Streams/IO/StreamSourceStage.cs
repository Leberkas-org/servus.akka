using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Servus.Akka.Streams.IO;

internal sealed class StreamSourceStage : GraphStage<SourceShape<ReadOnlyMemory<byte>>>
{
    private readonly Stream _stream;
    private readonly int _bufferSize;
    private readonly Outlet<ReadOnlyMemory<byte>> _out = new("StreamSource.Out");

    public StreamSourceStage(Stream stream, int bufferSize = 8 * 1024)
    {
        _stream = stream;
        _bufferSize = bufferSize;
        Shape = new SourceShape<ReadOnlyMemory<byte>>(_out);
    }

    public override SourceShape<ReadOnlyMemory<byte>> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

    private sealed record ReadCompleted(int BytesRead);

    private sealed record ReadFailed(Exception Error);

    [ExcludeFromCodeCoverage]
    private sealed class Logic : GraphStageLogic
    {
        private readonly StreamSourceStage _stage;
        private IActorRef _stageActor = ActorRefs.Nobody;
        private byte[] _readBuffer = [];

        public Logic(StreamSourceStage stage) : base(stage.Shape)
        {
            _stage = stage;
            SetHandler(stage._out, onPull: OnPull);
        }

        public override void PreStart()
        {
            _stageActor = GetStageActor(OnMessage).Ref;
            _readBuffer = new byte[_stage._bufferSize];
        }

        private void OnPull()
        {
            var vt = _stage._stream.ReadAsync(_readBuffer);

            vt.PipeTo(_stageActor,
                success: bytesRead => new ReadCompleted(bytesRead),
                failure: ex => new ReadFailed(ex));
        }

        private void OnMessage((IActorRef sender, object msg) args)
        {
            switch (args.msg)
            {
                case ReadCompleted completed:
                    ProcessBytesRead(completed.BytesRead);
                    break;

                case ReadFailed failed:
                    FailStage(failed.Error);
                    break;
            }
        }

        private void ProcessBytesRead(int bytesRead)
        {
            if (bytesRead == 0)
            {
                CompleteStage();
                return;
            }

            var copy = new byte[bytesRead];
            _readBuffer.AsSpan(0, bytesRead).CopyTo(copy);
            Push(_stage._out, copy.AsMemory());
        }

        public override void PostStop()
        {
            _readBuffer = [];
        }
    }
}