using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicStreamStateSpec
{
    [Fact(Timeout = 5000)]
    public void New_state_should_be_Opening()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.False(state.HasHandle);
    }

    [Fact(Timeout = 5000)]
    public void Write_in_Opening_should_buffer()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
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
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.CompleteWrites();

        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.True(state.IsCompleteWritesDeferred);
    }

    [Fact(Timeout = 5000)]
    public void AttachHandle_should_transition_to_Active()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        var handle = new StreamHandle(new MemoryStream());

        state.AttachHandle(handle);

        Assert.Equal(StreamPhase.Active, state.Phase);
        Assert.True(state.HasHandle);
    }

    [Fact(Timeout = 5000)]
    public void AttachHandle_should_flush_pending_writes()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0x01;
        buf.FullMemory.Span[1] = 0x02;
        buf.Length = 2;
        state.Write(buf);

        var handle = new StreamHandle(new MemoryStream());
        state.AttachHandle(handle);

        Assert.Equal(0, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public void AttachHandle_with_deferred_CompleteWrites_should_transition_to_HalfClosedWrite()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.CompleteWrites();

        state.AttachHandle(new StreamHandle(new MemoryStream()));

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_Active_should_transition_to_HalfClosedWrite()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));

        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_HalfClosedWrite_should_transition_to_Closed()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));
        state.CompleteWrites();

        state.OnReadCompleted();

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_Active_should_transition_to_HalfClosedRead()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));

        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Abort_should_transition_to_Closed()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));

        state.Abort(0);

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void DisposePendingWrites_should_clear_buffered_writes()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        var buf1 = TransportBuffer.Rent(2);
        buf1.FullMemory.Span[0] = 0x01;
        buf1.FullMemory.Span[1] = 0x02;
        buf1.Length = 2;
        state.Write(buf1);

        Assert.Equal(1, state.PendingWriteCount);

        // Dispose is called indirectly through DisposeAsync
        // We test by disposing the state and verifying buffers are released
        _ = state.DisposeAsync();

        // After dispose, pending writes should be cleared
        Assert.Equal(0, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public async ValueTask DisposeAsync_should_clean_up_handle()
    {
        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);
        var state = new QuicStreamState(StreamDirection.Bidirectional);

        state.AttachHandle(handle);
        Assert.True(state.HasHandle);

        await state.DisposeAsync();

        // After dispose, handle should be cleaned up (internal _handle = null)
        // We verify indirectly: another dispose should not throw
        await state.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public void Abort_in_Opening_should_transition_to_Closed()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);

        state.Abort(0);

        Assert.Equal(StreamPhase.Closed, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Multiple_buffered_writes_should_all_be_flushed()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);

        var buf1 = TransportBuffer.Rent(1);
        buf1.FullMemory.Span[0] = 0x01;
        buf1.Length = 1;
        state.Write(buf1);

        var buf2 = TransportBuffer.Rent(1);
        buf2.FullMemory.Span[0] = 0x02;
        buf2.Length = 1;
        state.Write(buf2);

        var buf3 = TransportBuffer.Rent(1);
        buf3.FullMemory.Span[0] = 0x03;
        buf3.Length = 1;
        state.Write(buf3);

        Assert.Equal(3, state.PendingWriteCount);

        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);
        state.AttachHandle(handle);

        Assert.Equal(0, state.PendingWriteCount);
        Assert.Equal(3, stream.Length);
    }

    [Fact(Timeout = 5000)]
    public void Write_in_Active_should_write_to_handle_directly()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);

        state.AttachHandle(handle);

        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0xAA;
        buf.FullMemory.Span[1] = 0xBB;
        buf.Length = 2;

        state.Write(buf);

        Assert.Equal(2, stream.Length);
        Assert.Equal(0xAA, stream.GetBuffer()[0]);
        Assert.Equal(0xBB, stream.GetBuffer()[1]);
    }

    [Fact(Timeout = 5000)]
    public void Write_in_HalfClosedWrite_still_writes_to_handle()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);

        state.AttachHandle(handle);
        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);

        var buf = TransportBuffer.Rent(2);
        buf.FullMemory.Span[0] = 0xCC;
        buf.FullMemory.Span[1] = 0xDD;
        buf.Length = 2;

        state.Write(buf);

        // Write still goes to handle (no phase check in Write method)
        Assert.Equal(2, stream.Length);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_in_HalfClosedWrite_should_be_no_op()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));
        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);

        // Calling again should not change phase
        state.CompleteWrites();

        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void OnReadCompleted_in_HalfClosedRead_should_stay_in_HalfClosedRead()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);
        state.AttachHandle(new StreamHandle(new MemoryStream()));
        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);

        // Calling again should be idempotent
        state.OnReadCompleted();

        Assert.Equal(StreamPhase.HalfClosedRead, state.Phase);
    }

    [Fact(Timeout = 5000)]
    public void Direction_should_return_construction_value()
    {
        var stateBidirectional = new QuicStreamState(StreamDirection.Bidirectional);
        Assert.Equal(StreamDirection.Bidirectional, stateBidirectional.Direction);

        var stateUnidirectional = new QuicStreamState(StreamDirection.Unidirectional);
        Assert.Equal(StreamDirection.Unidirectional, stateUnidirectional.Direction);
    }

    [Fact(Timeout = 5000)]
    public void AttachHandle_with_deferred_writes_and_deferred_CompleteWrites()
    {
        var state = new QuicStreamState(StreamDirection.Bidirectional);

        // Buffer writes
        var buf1 = TransportBuffer.Rent(1);
        buf1.FullMemory.Span[0] = 0x11;
        buf1.Length = 1;
        state.Write(buf1);

        var buf2 = TransportBuffer.Rent(1);
        buf2.FullMemory.Span[0] = 0x22;
        buf2.Length = 1;
        state.Write(buf2);

        // Defer CompleteWrites
        state.CompleteWrites();

        Assert.Equal(2, state.PendingWriteCount);
        Assert.True(state.IsCompleteWritesDeferred);

        // Attach handle - should flush writes then complete them
        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);
        state.AttachHandle(handle);

        // All writes should be flushed
        Assert.Equal(0, state.PendingWriteCount);
        Assert.Equal(2, stream.Length);

        // CompleteWrites should have been called, transitioning to HalfClosedWrite
        Assert.Equal(StreamPhase.HalfClosedWrite, state.Phase);
        Assert.False(state.IsCompleteWritesDeferred);
    }
}
