using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class SocketPipeConnectionSpec : IAsyncLifetime
{
    private Socket _listener = null!;
    private Socket _client = null!;
    private Socket _server = null!;

    public async ValueTask InitializeAsync()
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _listener.Listen(1);

        var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
        _client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await _client.ConnectAsync(endpoint, TestContext.Current.CancellationToken);
        _server = await _listener.AcceptAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _server.Dispose();
        _client.Dispose();
        _listener.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact(Timeout = 5000)]
    public async Task InputReader_should_receive_socket_data()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        var sent = "hello from server"u8.ToArray();
        await _server.SendAsync(sent, SocketFlags.None, TestContext.Current.CancellationToken);

        var result = await connection.InputReader.ReadAsync(TestContext.Current.CancellationToken);
        var received = System.Text.Encoding.UTF8.GetString(result.Buffer.FirstSpan);
        connection.InputReader.AdvanceTo(result.Buffer.End);

        Assert.Equal("hello from server", received);
    }

    [Fact(Timeout = 5000)]
    public async Task OutputWriter_should_send_data_to_socket()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        var data = "hello from client"u8.ToArray();
        await connection.OutputWriter.WriteAsync(data, TestContext.Current.CancellationToken);

        var buffer = new byte[1024];
        var received = await _server.ReceiveAsync(buffer, SocketFlags.None, TestContext.Current.CancellationToken);

        Assert.Equal("hello from client", System.Text.Encoding.UTF8.GetString(buffer, 0, received));
    }

    [Fact(Timeout = 5000)]
    public async Task InputReader_should_complete_on_socket_close()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        _server.Shutdown(SocketShutdown.Send);

        var result = await connection.InputReader.ReadAsync(TestContext.Current.CancellationToken);

        Assert.True(result.IsCompleted);
        Assert.True(result.Buffer.IsEmpty);
        connection.InputReader.AdvanceTo(result.Buffer.End);
    }

    [Fact(Timeout = 10000)]
    public async Task Backpressure_should_pause_socket_reads_when_pipe_full()
    {
        var options = new SocketPipeConnectionOptions
        {
            InputPauseWriterThreshold = 1024,
            InputResumeWriterThreshold = 512,
            WaitForData = false
        };

        await using var connection = SocketPipeConnection.Create(_client, options);

        // Send 2KB which exceeds the 1KB pause threshold
        var payload = new byte[2 * 1024];
        Array.Fill(payload, (byte)'X');
        await _server.SendAsync(payload, SocketFlags.None, TestContext.Current.CancellationToken);

        // Give the receive loop time to read and hit backpressure
        await Task.Delay(200, TestContext.Current.CancellationToken);

        // Read data from the pipe to verify it arrived
        var totalRead = 0L;
        while (totalRead < payload.Length)
        {
            var result = await connection.InputReader.ReadAsync(TestContext.Current.CancellationToken);
            totalRead += result.Buffer.Length;
            connection.InputReader.AdvanceTo(result.Buffer.End);
        }

        Assert.Equal(payload.Length, totalRead);
    }

    [Fact(Timeout = 5000)]
    public async Task Create_with_stream_should_work_for_tls()
    {
        var stream = new NetworkStream(_client, ownsSocket: false);
        await using var connection = SocketPipeConnection.Create(stream);

        var sent = "stream data"u8.ToArray();
        await _server.SendAsync(sent, SocketFlags.None, TestContext.Current.CancellationToken);

        var result = await connection.InputReader.ReadAsync(TestContext.Current.CancellationToken);
        var received = System.Text.Encoding.UTF8.GetString(result.Buffer.FirstSpan);
        connection.InputReader.AdvanceTo(result.Buffer.End);

        Assert.Equal("stream data", received);

        // Verify output direction works too
        var outData = "stream reply"u8.ToArray();
        await connection.OutputWriter.WriteAsync(outData, TestContext.Current.CancellationToken);

        var buffer = new byte[1024];
        var bytesReceived = await _server.ReceiveAsync(buffer, SocketFlags.None, TestContext.Current.CancellationToken);
        Assert.Equal("stream reply", System.Text.Encoding.UTF8.GetString(buffer, 0, bytesReceived));
    }

    [Fact(Timeout = 5000)]
    public async Task CompleteAndDrainOutputAsync_should_send_all_buffered_data_before_completing()
    {
        var memStream = new MemoryStream();
        var connection = SocketPipeConnection.Create(memStream);

        var writer = connection.OutputWriter;
        var mem = writer.GetMemory(4);
        mem.Span[0] = 0xAA;
        mem.Span[1] = 0xBB;
        mem.Span[2] = 0xCC;
        mem.Span[3] = 0xDD;
        writer.Advance(4);
        await writer.FlushAsync(TestContext.Current.CancellationToken);

        await connection.CompleteAndDrainOutputAsync();

        memStream.Position = 0;
        var data = new byte[4];
        var read = await memStream.ReadAsync(data, TestContext.Current.CancellationToken);
        Assert.Equal(4, read);
        Assert.Equal(0xAA, data[0]);
        Assert.Equal(0xDD, data[3]);

        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task OutputWriter_should_send_large_multisegment_payload_intact()
    {
        // A payload many times the pipe's MinimumSegmentSize forces the output pipe to span multiple
        // segments, exercising the send loop's multi-segment coalescing branch. Guards that the
        // coalesced bytes are written byte-for-byte regardless of which buffer pool backs the copy.
        var memStream = new MemoryStream();
        var connection = SocketPipeConnection.Create(memStream);

        var payload = new byte[256 * 1024];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i * 31 + 7);
        }

        await connection.OutputWriter.WriteAsync(payload, TestContext.Current.CancellationToken);
        await connection.CompleteAndDrainOutputAsync();

        Assert.Equal(payload, memStream.ToArray());

        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task CompleteAndDrainOutputAsync_should_send_large_multisegment_payload_intact()
    {
        // A payload many times the 16 KiB minimum segment size forces the output pipe to span
        // multiple buffer segments, exercising the send loop's multi-segment coalescing path.
        var memStream = new MemoryStream();
        var connection = SocketPipeConnection.Create(memStream);

        var payload = new byte[256 * 1024];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i % 251);
        }

        await connection.OutputWriter.WriteAsync(payload, TestContext.Current.CancellationToken);
        await connection.CompleteAndDrainOutputAsync();

        Assert.Equal(payload, memStream.ToArray());

        await connection.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task CompleteAndDrainOutputAsync_with_no_data_should_complete_without_error()
    {
        var memStream = new MemoryStream();
        var connection = SocketPipeConnection.Create(memStream);

        await connection.CompleteAndDrainOutputAsync();

        Assert.Equal(0, memStream.Length);

        await connection.DisposeAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task DisposeAsync_immediately_after_create_should_not_throw()
    {
        // Regression: the socket receive/send loops must not be scheduled via Task.Run(f, ct).
        // Disposing before the thread pool starts a loop delegate would otherwise cancel it
        // before its body runs, skipping the teardown catch/finally, and DisposeAsync's
        // Task.WhenAll would surface a TaskCanceledException. Immediate dispose maximises that
        // scheduling window; repeat to exercise the race. Post-fix this always completes cleanly.
        for (var i = 0; i < 50; i++)
        {
            var (client, server) = await CreateConnectedPairAsync(TestContext.Current.CancellationToken);
            try
            {
                var connection = SocketPipeConnection.Create(client);
                await connection.DisposeAsync();
            }
            finally
            {
                server.Dispose();
            }
        }
    }

    private static async Task<(Socket Client, Socket Server)> CreateConnectedPairAsync(CancellationToken ct)
    {
        using var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);

        var endpoint = (IPEndPoint)listener.LocalEndPoint!;
        var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await client.ConnectAsync(endpoint, ct);
        var server = await listener.AcceptAsync(ct);
        return (client, server);
    }
}
