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
        Assert.False(state.IsAttached);
    }

    [Fact(Timeout = 5000)]
    public void Write_in_Opening_should_buffer()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var buf = WireBuffer.Rent(2);
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
        Assert.True(state.IsAttached);
    }

    [Fact(Timeout = 5000)]
    public async Task AttachConnection_should_flush_pending_writes_to_output_pipe()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        var buf = WireBuffer.Rent(2);
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
        var buf = WireBuffer.Rent(2);
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
            var buf = WireBuffer.Rent(1);
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

        var buf = WireBuffer.Rent(2);
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

        var buf1 = WireBuffer.Rent(1);
        buf1.FullMemory.Span[0] = 0x11;
        buf1.Length = 1;
        state.Write(buf1);

        var buf2 = WireBuffer.Rent(1);
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
        state.Write(new byte[] { 1, 2, 3 }.ToWireBuffer());
        Assert.Equal(1, state.PendingWriteCount);
    }

    [Fact(Timeout = 5000)]
    public void AttachConnection_with_non_QuicStream_should_still_attach_one_connection()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);

        // MemoryStream is not a QuicStream — but there is now ONE attach path (StreamConnection) for
        // both QuicStream and plain streams, so QuicStream stays null while the state is attached.
        state.AttachConnection(new MemoryStream());

        Assert.Null(state.QuicStream);
        Assert.True(state.IsAttached);
    }

    [Fact(Timeout = 5000)]
    public void ReadFailure_transform_should_be_a_pure_wrapper()
    {
        // The transform runs on an IO-completion thread and must not touch mutable state — it only
        // packages the exception and the state reference; buffer release and QuicException
        // classification happen on the actor (the shared read handler / state machine failure routing).
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(7);

        var ioEx = new IOException("network error");
        var result = state.ReadFailure(ioEx);

        var failed = Assert.IsType<StreamReceiveFailed>(result);
        Assert.Same(ioEx, failed.Error);
        Assert.Same(state, failed.State);
    }

    [Fact(Timeout = 5000)]
    public void ReadSuccess_transform_should_be_a_pure_wrapper()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(42);

        var buf = WireBuffer.Rent(64);
        buf.Length = 16;
        var result = state.ReadSuccess(buf);

        var complete = Assert.IsType<StreamReceiveCompleted>(result);
        Assert.Same(state, complete.State);
        Assert.Same(buf, complete.Buffer);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void ReadSuccess_transform_should_wrap_null_for_eof()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(42);

        var result = state.ReadSuccess(null);

        var complete = Assert.IsType<StreamReceiveCompleted>(result);
        Assert.Same(state, complete.State);
        Assert.Null(complete.Buffer);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAndReturn_with_read_in_flight_should_quiesce_the_pending_receive()
    {
        // A read parked inside the connection's Stream.ReadAsync must be cancelled and settled by
        // DisposeAndReturnAsync's quiesce step — otherwise teardown would hang, or race a read still
        // writing into a pooled array. The repool round-trip itself is covered deterministically by
        // ObjectPoolSpec; asserting instance identity through the process-wide shared pool here is
        // inherently racy under parallel tests and is deliberately not attempted.
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.AttachConnection(new BlockingStream());

        // Park a receive inside the connection's Stream.ReadAsync — it blocks on Timeout.Infinite until
        // quiesce cancels it.
        var receive = state.ReceiveAsync();
        Assert.False(receive.IsCompleted);

        // Quiesce must cancel the parked read and complete within the test timeout (i.e. not hang).
        await state.DisposeAndReturnAsync();

        // The parked receive is now settled — never a dangling task. It either observes EOF (null) or a
        // teardown exception; both are acceptable graceful outcomes of cancellation.
        try
        {
            var buffer = await receive;
            buffer?.Dispose();
        }
        catch (Exception ex) when (ConnectionErrors.IsTeardown(ex))
        {
        }
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAndReturnAsync_without_read_in_flight_should_reset_state()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(17);

        await state.DisposeAndReturnAsync();

        Assert.Equal(StreamPhase.Opening, state.Phase);
        Assert.False(state.IsAttached);
    }

    /// <summary>Read parks forever until the receive is cancelled; write is a no-op sink.</summary>
    private sealed class BlockingStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => 0;
        public override long Position { get => 0; set { } }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return 0;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => 0;
        public override void Write(byte[] buffer, int offset, int count) { }
        public override long Seek(long offset, SeekOrigin origin) => 0;
        public override void SetLength(long value) { }
    }
}
