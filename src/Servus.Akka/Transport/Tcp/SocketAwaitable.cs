using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace Servus.Akka.Transport.Tcp;

internal sealed class SocketAwaitable()
    : SocketAsyncEventArgs(unsafeSuppressExecutionContextFlow: true), IValueTaskSource<int>
{
    private ManualResetValueTaskSourceCore<int> _core;

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