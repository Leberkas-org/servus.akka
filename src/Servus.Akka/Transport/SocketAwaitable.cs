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
        // A prior gather send may have left BufferList set; SocketAsyncEventArgs forbids Buffer and
        // BufferList being set at once, so clear it before switching to single-buffer mode.
        if (BufferList is not null)
        {
            BufferList = null;
        }

        SetBuffer(MemoryMarshal.AsMemory(buffer));
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

    // Vectored send: hand the whole multi-segment sequence to the kernel as a single scatter-gather
    // writev instead of one syscall per segment. Single-segment buffers fall through to the simple
    // single-buffer path. This is the path that lets pipelined responses leave in one socket write.
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

    private void SetBufferList(in ReadOnlySequence<byte> buffers)
    {
        var list = _bufferList ??= new List<ArraySegment<byte>>();
        list.Clear();

        foreach (var segment in buffers)
        {
            // Pipe segments are rented from MemoryPool<byte>.Shared, which is array-backed, so
            // TryGetArray always succeeds. Guard anyway so a non-array segment fails loudly rather
            // than silently sending nothing.
            if (!MemoryMarshal.TryGetArray(segment, out var array))
            {
                throw new InvalidOperationException("Send buffer segment is not backed by an array.");
            }

            list.Add(array);
        }

        // SocketAsyncEventArgs forbids Buffer and BufferList being set at once; clear any single
        // buffer left by a prior send before assigning the gather list.
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