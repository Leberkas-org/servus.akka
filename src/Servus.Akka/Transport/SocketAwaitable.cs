using System.Buffers;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace Servus.Akka.Transport;

internal sealed class SocketAwaitable()
    : SocketAsyncEventArgs(unsafeSuppressExecutionContextFlow: true), IValueTaskSource<int>
{
    private ManualResetValueTaskSourceCore<int> _core;
    private List<ArraySegment<byte>>? _bufferList;
    private int _maxBytesPerSend;

    internal void Configure(TransportConnectionOptions options)
    {
        _maxBytesPerSend = options.MaxBytesPerSend ?? 0;
    }

    public ValueTask<int> ReceiveAsync(Socket socket, Memory<byte> buffer)
    {
        _core.Reset();
        SetBuffer(buffer);
        if (socket.ReceiveAsync(this))
        {
            return new ValueTask<int>(this, _core.Version);
        }

        if (SocketError != SocketError.Success)
        {
            return ValueTask.FromException<int>(new SocketException((int)SocketError));
        }

        return new ValueTask<int>(BytesTransferred);
    }

    public ValueTask<int> WaitForDataAsync(Socket socket)
    {
        _core.Reset();
        SetBuffer(Memory<byte>.Empty);
        if (socket.ReceiveAsync(this))
        {
            return new ValueTask<int>(this, _core.Version);
        }

        return new ValueTask<int>(BytesTransferred);
    }

    public ValueTask<int> SendAsync(Socket socket, ReadOnlyMemory<byte> buffer)
    {
        _core.Reset();
        if (BufferList is not null)
        {
            BufferList = null;
        }

        var toSend = MemoryMarshal.AsMemory(buffer);
        if (_maxBytesPerSend > 0 && toSend.Length > _maxBytesPerSend)
        {
            toSend = toSend[.._maxBytesPerSend];
        }

        SetBuffer(toSend);
        if (socket.SendAsync(this))
        {
            return new ValueTask<int>(this, _core.Version);
        }

        if (SocketError != SocketError.Success)
        {
            return ValueTask.FromException<int>(new SocketException((int)SocketError));
        }

        return new ValueTask<int>(BytesTransferred);
    }

    public ValueTask<int> SendAsync(Socket socket, in ReadOnlySequence<byte> buffers)
    {
        if (buffers.IsSingleSegment)
        {
            return SendAsync(socket, buffers.First);
        }

        _core.Reset();
        SetBufferList(buffers);
        if (socket.SendAsync(this))
        {
            return new ValueTask<int>(this, _core.Version);
        }

        if (SocketError != SocketError.Success)
        {
            return ValueTask.FromException<int>(new SocketException((int)SocketError));
        }

        return new ValueTask<int>(BytesTransferred);
    }

    public async ValueTask<int> SendManyAsync(Socket socket, IReadOnlyList<WireBuffer> buffers)
    {
        var total = 0;
        for (var i = 0; i < buffers.Count; i++)
        {
            total += buffers[i].Length;
        }

        var remaining = total;
        var startIndex = 0;
        var startOffset = 0;

        while (remaining > 0)
        {
            FillBufferList(buffers, startIndex, startOffset);

            _core.Reset();
            int transferred;
            if (socket.SendAsync(this))
            {
                transferred = await new ValueTask<int>(this, _core.Version);
            }
            else if (SocketError != SocketError.Success)
            {
                throw new SocketException((int)SocketError);
            }
            else
            {
                transferred = BytesTransferred;
            }

            remaining -= transferred;
            if (remaining <= 0)
            {
                break;
            }

            var advance = transferred;
            while (advance > 0)
            {
                var available = buffers[startIndex].Length - startOffset;
                if (advance < available)
                {
                    startOffset += advance;
                    advance = 0;
                }
                else
                {
                    advance -= available;
                    startIndex++;
                    startOffset = 0;
                }
            }
        }

        return total;
    }

    private void FillBufferList(IReadOnlyList<WireBuffer> buffers, int startIndex, int startOffset)
    {
        var list = _bufferList ??= new List<ArraySegment<byte>>();
        list.Clear();

        var budget = _maxBytesPerSend > 0 ? _maxBytesPerSend : int.MaxValue;

        for (var i = startIndex; i < buffers.Count && budget > 0; i++)
        {
            var buffer = buffers[i];
            if (buffer.Length == 0)
            {
                continue;
            }

            if (!MemoryMarshal.TryGetArray<byte>(buffer.Memory, out var array))
            {
                throw new InvalidOperationException("Send buffer segment is not backed by an array.");
            }

            var segOffset = i == startIndex ? array.Offset + startOffset : array.Offset;
            var segCount = i == startIndex ? array.Count - startOffset : array.Count;
            if (segCount > budget)
            {
                segCount = budget;
            }

            list.Add(new ArraySegment<byte>(array.Array!, segOffset, segCount));
            budget -= segCount;
        }

        SetBuffer(null, 0, 0);
        BufferList = list;
    }

    private void SetBufferList(in ReadOnlySequence<byte> buffers)
    {
        var list = _bufferList ??= new List<ArraySegment<byte>>();
        list.Clear();

        foreach (var segment in buffers)
        {
            if (!MemoryMarshal.TryGetArray(segment, out var array))
            {
                throw new InvalidOperationException("Send buffer segment is not backed by an array.");
            }

            list.Add(array);
        }

        SetBuffer(null, 0, 0);
        BufferList = list;
    }

    protected override void OnCompleted(SocketAsyncEventArgs e)
    {
        if (SocketError != SocketError.Success)
        {
            _core.SetException(new SocketException((int)SocketError));
        }
        else
        {
            _core.SetResult(BytesTransferred);
        }
    }

    int IValueTaskSource<int>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);
}
