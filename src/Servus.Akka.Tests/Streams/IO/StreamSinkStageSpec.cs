using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using Servus.Akka.Streams.IO;

namespace Servus.Akka.Tests.Streams.IO;

public sealed class StreamSinkStageSpec : TestKit
{
    private readonly IMaterializer _materializer;

    public StreamSinkStageSpec() : base(ActorSystem.Create("test"))
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_write_single_chunk_to_stream()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var data = new byte[] { 1, 2, 3, 4, 5 };
        await Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        Assert.Equal(data, stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_write_multiple_chunks_to_stream()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var chunks = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5, 6 },
            new byte[] { 7, 8, 9 }
        };

        var task = Source.From(chunks.Select(c => (ReadOnlyMemory<byte>)c.AsMemory()))
            .RunWith(sink, _materializer);

        await task;

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_skip_empty_chunks()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var chunks = new[]
        {
            ReadOnlyMemory<byte>.Empty,
            (ReadOnlyMemory<byte>)new byte[] { 1, 2, 3 },
            ReadOnlyMemory<byte>.Empty,
            (ReadOnlyMemory<byte>)new byte[] { 4, 5 },
            ReadOnlyMemory<byte>.Empty
        };

        var task = Source.From(chunks)
            .RunWith(sink, _materializer);

        await task;

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_complete_task_when_upstream_finishes()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var task = Source.Empty<ReadOnlyMemory<byte>>()
            .RunWith(sink, _materializer);

        await task;

        Assert.Empty(stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_fault_task_when_upstream_fails()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var error = new InvalidOperationException("upstream failure");
        var task = Source.Failed<ReadOnlyMemory<byte>>(error)
            .RunWith(sink, _materializer);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Equal("upstream failure", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_synchronous_write_completion()
    {
        var stream = new SynchronousMemoryStream();
        var sink = StreamSink.To(stream);

        var data = new byte[] { 10, 20, 30 };
        await Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        Assert.Equal(data, stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_multiple_elements()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var items = new[]
        {
            (ReadOnlyMemory<byte>)new byte[] { 40 }.AsMemory(),
            (ReadOnlyMemory<byte>)new byte[] { 50 }.AsMemory(),
            (ReadOnlyMemory<byte>)new byte[] { 60 }.AsMemory()
        };

        await Source.From(items).RunWith(sink, _materializer);

        Assert.Equal(new byte[] { 40, 50, 60 }, stream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_continuous_writes()
    {
        var stream = new MemoryStream();
        var sink = StreamSink.To(stream);

        var chunks = new[]
        {
            new byte[] { 1, 2 },
            new byte[] { 3, 4 },
            new byte[] { 5, 6 }
        };

        await Source.From(chunks.Select(c => (ReadOnlyMemory<byte>)c.AsMemory()))
            .RunWith(sink, _materializer);

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, stream.ToArray());
    }

    private sealed class SynchronousMemoryStream : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return default;
        }

        public override Task FlushAsync(CancellationToken cancellationToken = default)
        {
            Flush();
            return Task.CompletedTask;
        }
    }

    private sealed class SlowMemoryStream : MemoryStream
    {
        private readonly int _delayMs;

        public SlowMemoryStream(int delayMs)
        {
            _delayMs = delayMs;
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delayMs, cancellationToken);
            Write(buffer.Span);
        }

        public override async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delayMs, cancellationToken);
            Flush();
        }
    }

    private sealed class FailingMemoryStream : MemoryStream
    {
        private bool _failOnFirstWrite;

        public FailingMemoryStream(bool failOnFirstWrite)
        {
            _failOnFirstWrite = failOnFirstWrite;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_failOnFirstWrite)
            {
                _failOnFirstWrite = false;
                return new ValueTask(Task.FromException(new InvalidOperationException("Write failed")));
            }

            Write(buffer.Span);
            return default;
        }
    }

    private sealed class FailingFlushMemoryStream : MemoryStream
    {
        public override async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Flush failed");
        }
    }
}
