using System.Net.Sockets;

namespace Servus.Akka.Transport;

/// <summary>
/// Plaintext-TCP duplex connection. Inbound is probe-gated rent-and-receive: a zero-byte probe
/// (the only cancellation point) parks idle connections without pinning a buffer, then the data
/// receive rents a <see cref="WireBuffer"/> and reads into it. Outbound coalesces batches into
/// vectored sends via <see cref="SocketAwaitable.SendManyAsync"/>.
/// </summary>
internal sealed class RawSocketConnection : DuplexConnectionBase
{
    private readonly Socket _socket;
    private readonly SocketAwaitable _receiver = new();
    private readonly SocketAwaitable _sender = new();

    public RawSocketConnection(Socket socket, TransportConnectionOptions options)
        : base(options.ReceiveBufferHint)
    {
        _socket = socket;
        _sender.Configure(options);
    }

    internal RawSocketConnection(Socket socket, TransportConnectionOptions options, Task? sendLoopStartGate)
        : base(options.ReceiveBufferHint, sendLoopStartGate)
    {
        _socket = socket;
        _sender.Configure(options);
    }

    protected override async ValueTask<WireBuffer?> ReceiveDataAsync(CancellationToken ct)
    {
        await _socket.ReceiveAsync(Memory<byte>.Empty, SocketFlags.None, ct);

        var buffer = WireBuffer.Rent(ReceiveHint);
        try
        {
            var bytesRead = await _receiver.ReceiveAsync(_socket, buffer.FullMemory);
            if (bytesRead == 0)
            {
                buffer.Dispose();
                return null;
            }

            buffer.Length = bytesRead;
            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    protected override async ValueTask<int> WriteBatchAsync(List<WireBuffer> batch, CancellationToken ct)
    {
        if (batch.Count == 1)
        {
            return await SendSingleAsync(batch[0]);
        }

        return await _sender.SendManyAsync(_socket, batch);
    }

    private async ValueTask<int> SendSingleAsync(WireBuffer buffer)
    {
        var memory = buffer.Memory;
        var offset = 0;

        while (offset < memory.Length)
        {
            var sent = await _sender.SendAsync(_socket, memory[offset..]);
            if (sent == 0)
            {
                throw new IOException("Socket send returned 0 bytes.");
            }

            offset += sent;
        }

        return memory.Length;
    }

    protected override ValueTask PreDrainShutdownAsync()
    {
        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
            // noop
        }

        _socket.Close();
        return default;
    }
}
