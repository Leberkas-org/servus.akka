namespace Servus.Akka.Transport.Quic;

internal sealed class StreamHandle : IAsyncDisposable
{
    private readonly Stream _stream;

    internal StreamHandle(Stream stream)
    {
        _stream = stream;
    }

    public void Write(TransportBuffer buffer)
    {
        var memory = buffer.Memory;
        _stream.Write(memory.Span);
        buffer.Dispose();
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct)
    {
        return _stream.ReadAsync(buffer, ct);
    }

    public void CompleteWrites()
    {
        if (_stream is System.Net.Quic.QuicStream qs)
        {
            qs.CompleteWrites();
        }
    }

    public void Abort(long errorCode)
    {
        if (_stream is System.Net.Quic.QuicStream qs)
        {
            qs.Abort(System.Net.Quic.QuicAbortDirection.Both, errorCode);
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}