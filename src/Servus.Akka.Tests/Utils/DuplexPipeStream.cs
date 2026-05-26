using System.IO.Pipelines;

namespace Servus.Akka.Tests.Utils;

public sealed class DuplexPipeStream(PipeReader reader, PipeWriter writer) : Stream
{
    private bool _disposed;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        if (_disposed)
        {
            return 0;
        }

        var result = await reader.ReadAsync(ct);
        var sequence = result.Buffer;

        if (sequence.IsEmpty && result.IsCompleted)
        {
            return 0;
        }

        var bytesToCopy = (int)Math.Min(buffer.Length, sequence.Length);
        var sliced = sequence.Slice(0, bytesToCopy);
        foreach (var segment in sliced)
        {
            segment.Span.CopyTo(buffer.Span);
            buffer = buffer[(int)segment.Length..];
        }

        reader.AdvanceTo(sliced.End);
        return bytesToCopy;
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        await writer.WriteAsync(buffer, ct);
    }

    public override async Task FlushAsync(CancellationToken ct)
    {
        await writer.FlushAsync(ct);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
            writer.Complete();
            reader.Complete();
        }

        base.Dispose(disposing);
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}