using System.Text;

namespace Servus.Akka.Tests.Utils;

public sealed class MockProxyStream : Stream
{
    private readonly byte[] _responseBytes;
    private readonly MemoryStream _writeBuffer = new();
    private int _readPosition;
    private bool _responseWritten;

    public MockProxyStream(string response)
    {
        _responseBytes = Encoding.ASCII.GetBytes(response);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        _responseWritten = true;
        _readPosition = 0;
        await Task.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Use ReadAsync instead");
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_responseWritten)
        {
            await Task.Yield();
            return 0;
        }

        if (_readPosition >= _responseBytes.Length)
        {
            return 0;
        }

        var bytesToRead = Math.Min(buffer.Length, _responseBytes.Length - _readPosition);
        _responseBytes.AsMemory(_readPosition, bytesToRead).CopyTo(buffer);
        _readPosition += bytesToRead;

        return bytesToRead;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Use WriteAsync instead");
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _writeBuffer.WriteAsync(buffer, cancellationToken);
        await Task.CompletedTask;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public string GetRequestContent()
    {
        return Encoding.ASCII.GetString(_writeBuffer.ToArray());
    }
}