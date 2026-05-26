using System.IO.Pipelines;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using Servus.Akka.Streams.IO;

namespace Servus.Akka.Tests.Streams.IO;

public sealed class PipeSourceStageSpec : TestKit
{
    private readonly IMaterializer _materializer;

    public PipeSourceStageSpec() : base(ActorSystem.Create("test"))
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_emit_data_written_to_pipe()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        var data = new byte[] { 1, 2, 3, 4, 5 };
        await pipe.Writer.WriteAsync(data, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(data, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_complete_on_empty_pipe()
    {
        var pipe = new Pipe();
        await pipe.Writer.CompleteAsync();

        var source = PipeSource.From(pipe.Reader);

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Empty(result);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_emit_multiple_chunks()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);
        await pipe.Writer.FlushAsync(CancellationToken.None);
        await pipe.Writer.WriteAsync(new byte[] { 4, 5, 6 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_incremental_writes()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        var collectTask = source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        await pipe.Writer.WriteAsync(new byte[] { 10, 20 }, CancellationToken.None);
        await pipe.Writer.FlushAsync(CancellationToken.None);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        await pipe.Writer.WriteAsync(new byte[] { 30 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await collectTask;
        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 10, 20, 30 }, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_from_stream_should_emit_data()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var stream = new MemoryStream(data);

        var source = StreamSource.From(stream);

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(data, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_multi_segment_buffer()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);
        await pipe.Writer.FlushAsync(CancellationToken.None);
        await pipe.Writer.WriteAsync(new byte[] { 4, 5, 6 }, CancellationToken.None);
        await pipe.Writer.FlushAsync(CancellationToken.None);
        await pipe.Writer.WriteAsync(new byte[] { 7, 8, 9 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_empty_buffer_with_is_completed_true()
    {
        var pipe = new Pipe();
        await pipe.Writer.CompleteAsync();

        var source = PipeSource.From(pipe.Reader);

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Empty(result);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_continue_reading_on_empty_buffer_with_is_completed_false()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        var collectTask = source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await collectTask;
        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 1, 2, 3 }, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_read_failure()
    {
        var pipe = new FailingPipeReader();
        var source = new PipeSourceStage(pipe);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Source.FromGraph(source)
                .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);
        });

        Assert.Equal("Read failed", error.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_push_data_and_complete_when_is_completed_true()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        var collectTask = source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        await pipe.Writer.WriteAsync(new byte[] { 10, 20, 30 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await collectTask;
        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 10, 20, 30 }, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_synchronous_read_completion()
    {
        var data = new byte[] { 99, 88, 77 };
        var pipe = new Pipe();

        var writeTask = pipe.Writer.WriteAsync(data, CancellationToken.None);
        await writeTask;
        await pipe.Writer.CompleteAsync();

        var source = PipeSource.From(pipe.Reader);

        var result = await source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(data, combined);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_asynchronous_read_completion()
    {
        var pipe = new Pipe();
        var source = PipeSource.From(pipe.Reader);

        var collectTask = source
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        await pipe.Writer.WriteAsync(new byte[] { 50, 60 }, CancellationToken.None);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        await pipe.Writer.WriteAsync(new byte[] { 70, 80 }, CancellationToken.None);
        await pipe.Writer.CompleteAsync();

        var result = await collectTask;
        var combined = result.SelectMany(m => m.ToArray()).ToArray();
        Assert.Equal(new byte[] { 50, 60, 70, 80 }, combined);
    }

    private sealed class FailingPipeReader : PipeReader
    {
        public override void AdvanceTo(SequencePosition consumed)
        {
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
        }

        public override void CancelPendingRead()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override async ValueTask CompleteAsync(Exception? exception = null)
        {
            await ValueTask.CompletedTask;
        }

        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(10, cancellationToken);
            throw new InvalidOperationException("Read failed");
        }

        public override bool TryRead(out ReadResult result)
        {
            result = default;
            return false;
        }
    }
}