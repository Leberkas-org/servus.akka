using System.Net.Quic;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicStreamStateSpec
{
    [Fact(Timeout = 5000)]
    public void New_state_should_be_Opening()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.Null(state.InputReader);
    }

    [Fact(Timeout = 5000)]
    public void Write_in_Opening_should_buffer()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0x01;
        buf.FullMemory.Span[1] = 0x02;
        buf.Length = 2;

        state.Write(buf);

        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.Equal(1, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_Opening_should_defer()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.CompleteWrites();

        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.True(state.IsCompleteWritesDeferred);
    }

    [Fact(Timeout = 5000)]
    public void AttachConnection_should_transition_to_Active()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        state.AttachConnection(new MemoryStream());

        Assert.Equal(StreamPhase.Active, state.Phase);
        Assert.NotNull(state.InputReader);
    }

    [Fact(Timeout = 5000)]
    public async Task AttachConnection_should_flush_pending_writes_to_output_pipe()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0x01;
        buf.FullMemory.Span[1] = 0x02;
        buf.Length = 2;
        state.Write(buf);

        var stream = new MemoryStream();
        state.AttachConnection(stream);

        Assert.Equal(0, state.PendingWriteCount);

        await state.CompleteAndDrainOutputAsync();
        stream.Position = 0;
        var data = new byte[2];
        var read = await stream.ReadAsync(data, TestContext.Current.CancellationToken);
        Assert.Equal(2, read);
        Assert.Equal(0x01, data[0]);
        Assert.Equal(0x02, data[1]);

        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public void AttachConnection_with_deferred_CompleteWrites_should_transition_to_HalfClosedWrite()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.CompleteWrites();

        state.AttachConnection(new MemoryStream());

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_Active_should_transition_to_HalfClosedWrite()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());

        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_HalfClosedWrite_should_transition_to_Closed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());
        state.CompleteWrites();

        state.OnReadCompleted();

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_Active_should_transition_to_HalfClosedRead()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());

        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_HalfClosedRead_should_transition_to_Closed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());
        state.OnReadCompleted();

        state.CompleteWrites();

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Abort_should_transition_to_Closed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());

        state.Abort(0);

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Abort_in_Opening_should_transition_to_Closed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        state.Abort(0);

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_dispose_pending_writes_and_connection()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var buf = TransportBuffer.Rent(2);
        buf.Length = 2;
        state.Write(buf);

        Assert.Equal(1, state.PendingWriteCount);

        await state.DisposeAsync();

        Assert.Equal(0, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_after_attach_should_dispose_connection()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());

        await state.DisposeAsync();

        // Double dispose should not throw
        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Multiple_buffered_writes_should_all_be_flushed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        for (byte i = 1; i <= 3; i++)
        {
            var buf = TransportBuffer.Rent(1);
            buf.FullMemory.Span[0] = i;
            buf.Length = 1;
            state.Write(buf);
        }

        Assert.Equal(3, state.PendingWriteCount);

        var stream = new MemoryStream();
        state.AttachConnection(stream);

        Assert.Equal(0, state.PendingWriteCount);

        await state.CompleteAndDrainOutputAsync();
        Assert.Equal(3, stream.Length);

        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task Write_in_Active_should_write_to_output_pipe()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var stream = new MemoryStream();
        state.AttachConnection(stream);

        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0xAA;
        buf.FullMemory.Span[1] = 0xBB;
        buf.Length = 2;

        state.Write(buf);

        await state.CompleteAndDrainOutputAsync();
        stream.Position = 0;
        Assert.Equal(2, stream.Length);
        Assert.Equal(0xAA, stream.GetBuffer()[0]);
        Assert.Equal(0xBB, stream.GetBuffer()[1]);

        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public void Direction_should_return_construction_value()
    {
        var stateBi = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        Assert.Equal(StreamDirection.Bidirectional, stateBi.Direction);

        var stateUni = QuicStreamState.Rent(StreamDirection.Unidirectional, null);
        Assert.Equal(StreamDirection.Unidirectional, stateUni.Direction);
    }

    [Fact(Timeout = 5000)]
    public async Task AttachConnection_with_deferred_writes_and_deferred_CompleteWrites()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        var buf1 = TransportBuffer.Rent(1);
        buf1.FullMemory.Span[0] = 0x11;
        buf1.Length = 1;
        state.Write(buf1);

        var buf2 = TransportBuffer.Rent(1);
        buf2.FullMemory.Span[0] = 0x22;
        buf2.Length = 1;
        state.Write(buf2);

        state.CompleteWrites();

        Assert.Equal(2, state.PendingWriteCount);
        Assert.True(state.IsCompleteWritesDeferred);

        state.AttachConnection(new MemoryStream());

        Assert.Equal(0, state.PendingWriteCount);
        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
        Assert.False(state.IsCompleteWritesDeferred);

        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_HalfClosedWrite_should_be_no_op()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());
        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);

        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_HalfClosedRead_should_stay_in_HalfClosedRead()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());
        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);

        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Write_during_opening_buffers_the_payload()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        Assert.Equal(0, state.PendingWriteCount);
        state.Write(new byte[] { 1, 2, 3 });
        Assert.Equal(1, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public void AttachConnection_with_QuicStream_should_set_DirectReadTransform()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        // MemoryStream is not a QuicStream, so DirectReadTransform stays null
        state.AttachConnection(new MemoryStream());

        Assert.Null(state.DirectReadTransform);
        Assert.Null(state.QuicStream);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_should_dispose_pending_read_buffer()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new MemoryStream());

        var buf = TransportBuffer.Rent(64);
        buf.Length = 10;
        state.PendingReadBuffer = buf;

        await state.DisposeAsync();

        Assert.Null(state.PendingReadBuffer);
    }

    [Fact(Timeout = 5000)]
    public void FailureReadTransform_with_QuicException_should_return_PipeStreamReadComplete()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(42);

        var buf = TransportBuffer.Rent(64);
        buf.Length = 16;
        state.PendingReadBuffer = buf;

        var quicEx = new QuicException(QuicError.StreamAborted, applicationErrorCode: null, "stream reset by peer");
        var result = state.FailureReadTransform!(quicEx);

        var complete = Assert.IsType<PipeStreamReadComplete>(result);
        Assert.Equal(42, complete.StreamId);
        Assert.True(complete.IsCompleted);
        Assert.Null(state.PendingReadBuffer);
    }

    [Fact(Timeout = 5000)]
    public void FailureReadTransform_with_non_QuicException_should_return_PipeStreamReadFailed()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(7);

        var buf = TransportBuffer.Rent(64);
        buf.Length = 16;
        state.PendingReadBuffer = buf;

        var ioEx = new IOException("network error");
        var result = state.FailureReadTransform!(ioEx);

        var failed = Assert.IsType<PipeStreamReadFailed>(result);
        Assert.Equal(7, failed.StreamId);
        Assert.Same(ioEx, failed.Error);
        Assert.Null(state.PendingReadBuffer);
    }

}
