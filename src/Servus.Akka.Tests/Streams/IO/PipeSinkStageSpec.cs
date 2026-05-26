using System.IO.Pipelines;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using Servus.Akka.Streams.IO;

namespace Servus.Akka.Tests.Streams.IO;

public sealed class PipeSinkStageSpec : TestKit
{
    private readonly IMaterializer _materializer;

    public PipeSinkStageSpec() : base(ActorSystem.Create("test"))
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_write_data_to_pipe_reader()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var data = new byte[] { 1, 2, 3, 4, 5 };
        await Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
        Assert.Equal(data, readResult.Buffer.FirstSpan.ToArray());
        pipe.Reader.AdvanceTo(readResult.Buffer.End);

        var finalRead = await pipe.Reader.ReadAsync(CancellationToken.None);
        Assert.True(finalRead.IsCompleted);
        await pipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_write_multiple_chunks_to_pipe_reader()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var chunks = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5, 6 },
            new byte[] { 7, 8, 9 }
        };

        var writeTask = Source.From(chunks.Select(c => (ReadOnlyMemory<byte>)c.AsMemory()))
            .RunWith(sink, _materializer);

        var total = new List<byte>();
        while (true)
        {
            var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
            foreach (var segment in readResult.Buffer)
            {
                total.AddRange(segment.ToArray());
            }

            pipe.Reader.AdvanceTo(readResult.Buffer.End);

            if (readResult.IsCompleted)
            {
                break;
            }
        }

        await pipe.Reader.CompleteAsync();
        await writeTask;
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, total.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_complete_task_when_upstream_finishes()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var task = Source.Empty<ReadOnlyMemory<byte>>()
            .RunWith(sink, _materializer);

        await task;

        var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
        Assert.True(readResult.IsCompleted);
        Assert.True(readResult.Buffer.IsEmpty);
        await pipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_fault_task_when_upstream_fails()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var error = new InvalidOperationException("test failure");
        var task = Source.Failed<ReadOnlyMemory<byte>>(error)
            .RunWith(sink, _materializer);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Equal("test failure", ex.Message);
        await pipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_skip_empty_chunks()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var chunks = new[]
        {
            ReadOnlyMemory<byte>.Empty,
            (ReadOnlyMemory<byte>)new byte[] { 1, 2, 3 },
            ReadOnlyMemory<byte>.Empty
        };

        var writeTask = Source.From(chunks)
            .RunWith(sink, _materializer);

        var total = new List<byte>();
        while (true)
        {
            var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
            foreach (var segment in readResult.Buffer)
            {
                total.AddRange(segment.ToArray());
            }

            pipe.Reader.AdvanceTo(readResult.Buffer.End);

            if (readResult.IsCompleted)
            {
                break;
            }
        }

        await pipe.Reader.CompleteAsync();
        await writeTask;
        Assert.Equal(new byte[] { 1, 2, 3 }, total.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_to_stream_should_write_data()
    {
        var memoryStream = new MemoryStream();
        var sink = StreamSink.To(memoryStream);

        var data = new byte[] { 10, 20, 30 };
        await Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        Assert.Equal(data, memoryStream.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_empty_chunks()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var chunks = new[]
        {
            ReadOnlyMemory<byte>.Empty,
            (ReadOnlyMemory<byte>)new byte[] { 1, 2 },
            ReadOnlyMemory<byte>.Empty,
            (ReadOnlyMemory<byte>)new byte[] { 3, 4 },
            ReadOnlyMemory<byte>.Empty
        };

        var writeTask = Source.From(chunks)
            .RunWith(sink, _materializer);

        var total = new List<byte>();
        while (true)
        {
            var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
            foreach (var segment in readResult.Buffer)
            {
                total.AddRange(segment.ToArray());
            }

            pipe.Reader.AdvanceTo(readResult.Buffer.End);

            if (readResult.IsCompleted)
            {
                break;
            }
        }

        await pipe.Reader.CompleteAsync();
        await writeTask;
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, total.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_complete_on_normal_write()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var data = new byte[] { 5, 10, 15 };
        _ = Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        var result = await pipe.Reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Buffer.Length);
        pipe.Reader.AdvanceTo(result.Buffer.End);
        await pipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_flush_result_is_completed()
    {
        var pipe = new CompletedFlushResultPipe();
        var sink = PipeSink.To(pipe.Writer);

        var data = new byte[] { 20, 30 };
        var task = Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        await task;
        Assert.True(pipe.WriteWasCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_flush_result_is_canceled()
    {
        var pipe = new CanceledFlushResultPipe();
        var sink = PipeSink.To(pipe.Writer);

        var data = "(2"u8.ToArray();
        var task = Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        await task;
        Assert.True(pipe.WriteWasCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_upstream_failure()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var error = new InvalidOperationException("upstream error");
        var task = Source.Failed<ReadOnlyMemory<byte>>(error)
            .RunWith(sink, _materializer);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Equal("upstream error", ex.Message);
        await pipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_synchronous_write_completion()
    {
        var pipe = new SynchronousWritePipe();
        var sink = PipeSink.To(pipe.Writer);

        var data = "<F"u8.ToArray();
        var task = Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        await task;
        Assert.True(pipe.WriteWasCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_asynchronous_write_completion()
    {
        var pipe = new SlowWritePipe(delayMs: 50);
        var sink = PipeSink.To(pipe.Writer);

        var data = "PZ"u8.ToArray();
        var task = Source.Single((ReadOnlyMemory<byte>)data.AsMemory())
            .RunWith(sink, _materializer);

        await task;
        Assert.True(pipe.WriteWasCalled);
    }

    [Fact(Timeout = 5000)]
    public async Task Sink_should_handle_continuous_writes_with_flush_completion()
    {
        var pipe = new Pipe();
        var sink = PipeSink.To(pipe.Writer);

        var chunks = new[]
        {
            new byte[] { 1, 2 },
            new byte[] { 3, 4 },
            new byte[] { 5, 6 }
        };

        var writeTask = Source.From(chunks.Select(c => (ReadOnlyMemory<byte>)c.AsMemory()))
            .RunWith(sink, _materializer);

        var total = new List<byte>();
        while (true)
        {
            var readResult = await pipe.Reader.ReadAsync(CancellationToken.None);
            foreach (var segment in readResult.Buffer)
            {
                total.AddRange(segment.ToArray());
            }

            pipe.Reader.AdvanceTo(readResult.Buffer.End);

            if (readResult.IsCompleted)
            {
                break;
            }
        }

        await pipe.Reader.CompleteAsync();
        await writeTask;
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, total.ToArray());
    }

    private sealed class CompletedFlushResultPipe
    {
        private readonly Pipe _pipe = new();
        public bool WriteWasCalled { get; set; }

        public PipeWriter Writer { get; }

        public CompletedFlushResultPipe()
        {
            Writer = new CompletedResultPipeWriter(_pipe.Writer, this);
        }

        private sealed class CompletedResultPipeWriter : PipeWriter
        {
            private readonly PipeWriter _inner;
            private readonly CompletedFlushResultPipe _owner;

            public CompletedResultPipeWriter(PipeWriter inner, CompletedFlushResultPipe owner)
            {
                _inner = inner;
                _owner = owner;
            }

            public override void Advance(int bytes)
            {
                _inner.Advance(bytes);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _inner.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _inner.GetSpan(sizeHint);
            }

            public override ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                _owner.WriteWasCalled = true;
                return new ValueTask<FlushResult>(new FlushResult(isCompleted: true, isCanceled: false));
            }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                return await _inner.FlushAsync(cancellationToken);
            }

            public override void CancelPendingFlush()
            {
                _inner.CancelPendingFlush();
            }

            public override void Complete(Exception? exception = null)
            {
                _inner.Complete(exception);
            }

            public override async ValueTask CompleteAsync(Exception? exception = null)
            {
                await _inner.CompleteAsync(exception);
            }
        }
    }

    private sealed class CanceledFlushResultPipe
    {
        private readonly Pipe _pipe = new();
        public bool WriteWasCalled { get; set; }

        public PipeWriter Writer { get; }

        public CanceledFlushResultPipe()
        {
            Writer = new CanceledResultPipeWriter(_pipe.Writer, this);
        }

        private sealed class CanceledResultPipeWriter : PipeWriter
        {
            private readonly PipeWriter _inner;
            private readonly CanceledFlushResultPipe _owner;

            public CanceledResultPipeWriter(PipeWriter inner, CanceledFlushResultPipe owner)
            {
                _inner = inner;
                _owner = owner;
            }

            public override void Advance(int bytes)
            {
                _inner.Advance(bytes);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _inner.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _inner.GetSpan(sizeHint);
            }

            public override ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                _owner.WriteWasCalled = true;
                return new ValueTask<FlushResult>(new FlushResult(isCompleted: false, isCanceled: true));
            }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                return await _inner.FlushAsync(cancellationToken);
            }

            public override void CancelPendingFlush()
            {
                _inner.CancelPendingFlush();
            }

            public override void Complete(Exception? exception = null)
            {
                _inner.Complete(exception);
            }

            public override async ValueTask CompleteAsync(Exception? exception = null)
            {
                await _inner.CompleteAsync(exception);
            }
        }
    }

    private sealed class SynchronousWritePipe
    {
        private readonly Pipe _pipe = new();
        public bool WriteWasCalled { get; set; }

        public PipeWriter Writer { get; }

        public SynchronousWritePipe()
        {
            Writer = new SyncPipeWriter(_pipe.Writer, this);
        }

        private sealed class SyncPipeWriter : PipeWriter
        {
            private readonly PipeWriter _inner;
            private readonly SynchronousWritePipe _owner;

            public SyncPipeWriter(PipeWriter inner, SynchronousWritePipe owner)
            {
                _inner = inner;
                _owner = owner;
            }

            public override void Advance(int bytes)
            {
                _inner.Advance(bytes);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _inner.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _inner.GetSpan(sizeHint);
            }

            public override ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                _owner.WriteWasCalled = true;
                var span = _inner.GetSpan(buffer.Length);
                buffer.Span.CopyTo(span);
                _inner.Advance(buffer.Length);
                return new ValueTask<FlushResult>(new FlushResult(isCompleted: false, isCanceled: false));
            }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                return await _inner.FlushAsync(cancellationToken);
            }

            public override void CancelPendingFlush()
            {
                _inner.CancelPendingFlush();
            }

            public override void Complete(Exception? exception = null)
            {
                _inner.Complete(exception);
            }

            public override async ValueTask CompleteAsync(Exception? exception = null)
            {
                await _inner.CompleteAsync(exception);
            }
        }
    }

    private sealed class SlowWritePipe
    {
        private readonly Pipe _pipe = new();
        private readonly int _delayMs;
        public bool WriteWasCalled { get; set; }

        public PipeWriter Writer { get; }

        public SlowWritePipe(int delayMs)
        {
            _delayMs = delayMs;
            Writer = new SlowPipeWriter(_pipe.Writer, this, delayMs);
        }

        private sealed class SlowPipeWriter : PipeWriter
        {
            private readonly PipeWriter _inner;
            private readonly SlowWritePipe _owner;
            private readonly int _delayMs;

            public SlowPipeWriter(PipeWriter inner, SlowWritePipe owner, int delayMs)
            {
                _inner = inner;
                _owner = owner;
                _delayMs = delayMs;
            }

            public override void Advance(int bytes)
            {
                _inner.Advance(bytes);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _inner.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _inner.GetSpan(sizeHint);
            }

            public override async ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                _owner.WriteWasCalled = true;
                await Task.Delay(_delayMs, cancellationToken);
                var span = _inner.GetSpan(buffer.Length);
                buffer.Span.CopyTo(span);
                _inner.Advance(buffer.Length);
                return new FlushResult(isCompleted: false, isCanceled: false);
            }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                return await _inner.FlushAsync(cancellationToken);
            }

            public override void CancelPendingFlush()
            {
                _inner.CancelPendingFlush();
            }

            public override void Complete(Exception? exception = null)
            {
                _inner.Complete(exception);
            }

            public override async ValueTask CompleteAsync(Exception? exception = null)
            {
                await _inner.CompleteAsync(exception);
            }
        }
    }
}