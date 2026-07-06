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
    public void AttachConnection_with_non_QuicStream_should_use_pipe_read_path()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        // MemoryStream is not a QuicStream, so the direct-read path (QuicStream) stays unavailable.
        state.AttachConnection(new MemoryStream());

        Assert.Null(state.QuicStream);
        Assert.NotNull(state.InputReader);
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
    public void FailureReadTransform_should_be_a_pure_wrapper()
    {
        // The transform runs on an IO-completion thread and must not touch mutable state — it only
        // packages the exception and the state reference; buffer release and QuicException
        // classification happen on the actor (CompleteRead / the state machine's failure handler).
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(7);

        var buf = TransportBuffer.Rent(64);
        buf.Length = 16;
        state.BeginDirectRead(buf);

        var ioEx = new IOException("network error");
        var result = state.FailureReadTransform(ioEx);

        var failed = Assert.IsType<PipeStreamReadFailed>(result);
        Assert.Same(ioEx, failed.Error);
        Assert.Same(state, failed.State);
        Assert.Same(buf, state.PendingReadBuffer);
        Assert.True(state.ReadInFlight);

        Assert.True(state.CompleteRead(out var pending));
        pending!.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void DirectReadTransform_should_be_a_pure_wrapper()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(42);

        var buf = TransportBuffer.Rent(64);
        state.BeginDirectRead(buf);

        var result = state.DirectReadTransform(16);

        var complete = Assert.IsType<DirectStreamReadComplete>(result);
        Assert.Same(state, complete.State);
        Assert.Equal(16, complete.BytesRead);
        Assert.Same(buf, state.PendingReadBuffer);
        Assert.True(state.ReadInFlight);

        Assert.True(state.CompleteRead(out var pending));
        pending!.Dispose();
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAndReturnAsync_with_read_in_flight_should_defer_buffer_to_CompleteRead()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(11);

        var buf = TransportBuffer.Rent(64);
        state.BeginDirectRead(buf);

        // Teardown races an in-flight QuicStream.ReadAsync that is still writing into buf's memory:
        // the buffer must NOT be disposed here (returning its array to the shared pool while the
        // read writes into it corrupts whichever connection rents it next — the poisoned-pool bug
        // behind the GaudiHTTP 8 MB H2 download stall).
        await state.DisposeAndReturnAsync();

        Assert.True(state.ReadInFlight);
        Assert.NotNull(state.PendingReadBuffer);

        // When the completion event reaches the actor, CompleteRead releases the buffer exactly
        // once and reports the teardown so the event is dropped.
        Assert.False(state.CompleteRead(out var pending));
        Assert.Null(pending);
        Assert.Null(state.PendingReadBuffer);
        Assert.False(state.ReadInFlight);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAndReturnAsync_without_read_in_flight_should_reset_state()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(17);

        await state.DisposeAndReturnAsync();

        Assert.False(state.ReadInFlight);
        Assert.Null(state.PendingReadBuffer);
        Assert.Equal(StreamPhase.Opening, state.Phase);
    }
}
