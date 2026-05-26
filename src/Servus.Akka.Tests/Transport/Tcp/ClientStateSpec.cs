using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class ClientStateSpec
{
    [Fact(Timeout = 5000)]
    public void ClientState_should_dispose_stream_on_dispose()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        state.Dispose();

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_create_pipes_by_default()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        Assert.NotNull(state.InboundPipe);
        Assert.NotNull(state.OutboundPipe);
    }

    [Fact(Timeout = 5000)]
    public async Task ClientState_should_have_working_inbound_pipe()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        var writer = state.InboundPipe.Writer;
        var data = new byte[] { 1, 2, 3 };
        await writer.WriteAsync(data, TestContext.Current.CancellationToken);
        await writer.CompleteAsync();

        var result = await state.InboundPipe.Reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Buffer.Length);
        state.InboundPipe.Reader.AdvanceTo(result.Buffer.End);
        await state.InboundPipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task ClientState_should_have_working_outbound_pipe()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        var writer = state.OutboundPipe.Writer;
        var data = new byte[] { 4, 5, 6 };
        await writer.WriteAsync(data, TestContext.Current.CancellationToken);
        await writer.CompleteAsync();

        var result = await state.OutboundPipe.Reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Buffer.Length);
        state.OutboundPipe.Reader.AdvanceTo(result.Buffer.End);
        await state.OutboundPipe.Reader.CompleteAsync();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_expose_stream_property()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        Assert.Same(stream, state.Stream);
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_allow_on_writes_complete_callback()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream)
        {
            OnWritesComplete = () => { }
        };

        Assert.NotNull(state.OnWritesComplete);
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_complete_pipes_on_dispose()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        state.Dispose();

        Assert.Throws<InvalidOperationException>(() =>
        {
            state.InboundPipe.Writer.GetMemory(1);
        });
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_handle_double_dispose()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        state.Dispose();
        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_create_with_write_only_direction()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream, PipeMode.WriteOnly);

        Assert.Equal(PipeMode.WriteOnly, state.Direction);
        Assert.NotNull(state.OutboundPipe);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_create_with_read_only_direction()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream, PipeMode.ReadOnly);

        Assert.Equal(PipeMode.ReadOnly, state.Direction);
        Assert.NotNull(state.InboundPipe);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_default_to_bidirectional_direction()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        Assert.Equal(PipeMode.Bidirectional, state.Direction);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_expose_on_writes_complete_as_null_by_default()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        Assert.Null(state.OnWritesComplete);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_expose_channel_readers_and_writers()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        Assert.NotNull(state.InboundReader);
        Assert.NotNull(state.InboundWriter);
        Assert.NotNull(state.OutboundReader);
        Assert.NotNull(state.OutboundWriter);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_dispose_buffered_channel_items()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        var buf1 = TransportBuffer.Rent(4);
        buf1.Length = 4;
        var buf2 = TransportBuffer.Rent(4);
        buf2.Length = 4;

        state.InboundWriter.TryWrite(buf1);
        state.OutboundWriter.TryWrite(buf2);

        state.Dispose();

        Assert.False(state.InboundReader.TryRead(out _));
        Assert.False(state.OutboundReader.TryRead(out _));
    }

    [Fact(Timeout = 5000)]
    public async Task ClientState_should_handle_pre_completed_pipes_on_dispose()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        await state.InboundPipe.Writer.CompleteAsync();
        await state.InboundPipe.Reader.CompleteAsync();
        await state.OutboundPipe.Writer.CompleteAsync();
        await state.OutboundPipe.Reader.CompleteAsync();

        state.Dispose();

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_handle_InvalidOperationException_on_writer_complete()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        state.InboundPipe.Writer.Complete();
        state.OutboundPipe.Writer.Complete();

        // Should not throw - catches InvalidOperationException
        state.Dispose();

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_handle_InvalidOperationException_on_reader_complete()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        state.InboundPipe.Reader.Complete();
        state.OutboundPipe.Reader.Complete();

        // Should not throw - catches InvalidOperationException
        state.Dispose();

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_drain_multiple_buffered_inbound_items()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        var buf1 = TransportBuffer.Rent(10);
        buf1.Length = 10;
        var buf2 = TransportBuffer.Rent(10);
        buf2.Length = 10;
        var buf3 = TransportBuffer.Rent(10);
        buf3.Length = 10;

        state.InboundWriter.TryWrite(buf1);
        state.InboundWriter.TryWrite(buf2);
        state.InboundWriter.TryWrite(buf3);

        state.Dispose();

        // All buffers should be disposed via drain loop
        Assert.False(state.InboundReader.TryRead(out _));
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_drain_multiple_buffered_outbound_items()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        var buf1 = TransportBuffer.Rent(10);
        buf1.Length = 10;
        var buf2 = TransportBuffer.Rent(10);
        buf2.Length = 10;
        var buf3 = TransportBuffer.Rent(10);
        buf3.Length = 10;

        state.OutboundWriter.TryWrite(buf1);
        state.OutboundWriter.TryWrite(buf2);
        state.OutboundWriter.TryWrite(buf3);

        state.Dispose();

        // All buffers should be disposed via drain loop
        Assert.False(state.OutboundReader.TryRead(out _));
    }

    [Fact(Timeout = 5000)]
    public void ClientState_should_handle_exception_on_all_pipe_completions()
    {
        var stream = new MemoryStream();
        var state = new ClientState(stream);

        // Complete all pipes first
        state.InboundPipe.Writer.Complete();
        state.InboundPipe.Reader.Complete();
        state.OutboundPipe.Writer.Complete();
        state.OutboundPipe.Reader.Complete();

        // Attempting to complete again should not throw
        state.Dispose();
        state.Dispose(); // Double dispose

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }
}
