using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit;
using Servus.Akka.Streams.IO;

namespace Servus.Akka.Tests.Streams.IO;

public sealed class StreamSourceStageSpec : TestKit
{
    private readonly IMaterializer _materializer;

    public StreamSourceStageSpec() : base(ActorSystem.Create("test"))
    {
        _materializer = Sys.Materializer();
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_read_single_chunk_from_stream()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var stream = new MemoryStream(data);

        var result = await StreamSource.From(stream)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Equal(data, ConcatBytes(result));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_read_large_stream_in_multiple_chunks()
    {
        var data = new byte[32 * 1024];
        Random.Shared.NextBytes(data);
        var stream = new MemoryStream(data);

        var result = await StreamSource.From(stream, bufferSize: 4096)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.True(result.Count > 1);
        Assert.Equal(data, ConcatBytes(result));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_complete_on_empty_stream()
    {
        var stream = new MemoryStream([]);

        var result = await StreamSource.From(stream)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Empty(result);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_use_custom_buffer_size()
    {
        var data = new byte[100];
        Random.Shared.NextBytes(data);
        var stream = new MemoryStream(data);

        var result = await StreamSource.From(stream, bufferSize: 10)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.True(result.Count >= 10);
        Assert.Equal(data, ConcatBytes(result));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_fail_when_stream_throws()
    {
        var stream = new FailingReadStream();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await StreamSource.From(stream)
                .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer));

        Assert.Equal("Read failed", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_synchronous_read_completion()
    {
        var data = new byte[] { 10, 20, 30 };
        var stream = new SynchronousMemoryStream(data);

        var result = await StreamSource.From(stream)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Equal(data, ConcatBytes(result));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_read_exact_bytes_per_chunk()
    {
        var data = new byte[] { 1, 2, 3, 4, 5, 6 };
        var stream = new MemoryStream(data);

        var chunks = await StreamSource.From(stream, bufferSize: 3)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        foreach (var chunk in chunks)
        {
            Assert.True(chunk.Length <= 3);
        }

        Assert.Equal(data, ConcatBytes(chunks));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_handle_slow_reads()
    {
        var data = new byte[] { 1, 2, 3 };
        var stream = new SlowMemoryStream(data, delayMs: 50);

        var result = await StreamSource.From(stream)
            .RunWith(Sink.Seq<ReadOnlyMemory<byte>>(), _materializer);

        Assert.Equal(data, ConcatBytes(result));
    }

    [Fact(Timeout = 5000)]
    public async Task Source_should_propagate_through_flow()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var stream = new MemoryStream(data);

        var result = await StreamSource.From(stream)
            .Select(chunk => chunk.Length)
            .RunWith(Sink.Sum<int>((a, b) => a + b), _materializer);

        Assert.Equal(data.Length, result);
    }

    private static byte[] ConcatBytes(IReadOnlyList<ReadOnlyMemory<byte>> chunks)
    {
        var total = 0;
        foreach (var c in chunks) total += c.Length;

        var result = new byte[total];
        var offset = 0;
        foreach (var c in chunks)
        {
            c.Span.CopyTo(result.AsSpan(offset));
            offset += c.Length;
        }

        return result;
    }

    private sealed class SynchronousMemoryStream(byte[] data) : MemoryStream(data)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesRead = Read(buffer.Span);
            return new ValueTask<int>(bytesRead);
        }
    }

    private sealed class SlowMemoryStream(byte[] data, int delayMs) : MemoryStream(data)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(delayMs, cancellationToken);
            return Read(buffer.Span);
        }
    }

    private sealed class FailingReadStream : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return new ValueTask<int>(Task.FromException<int>(new InvalidOperationException("Read failed")));
        }
    }
}
