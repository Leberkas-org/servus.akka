using System.Diagnostics.CodeAnalysis;
using System.Net.Quic;

namespace Servus.Akka.Transport.Quic;

/// <summary>
/// The per-state-machine surface the shared read-completion handler needs. Both QUIC state machines
/// (client + server) implement it so <see cref="QuicStreamReads"/> can drive the identical read-pump
/// bookkeeping without duplicating it in each machine.
/// </summary>
internal interface IQuicStreamReadHost
{
    IConnectionOperations Ops { get; }

    bool TryGetStream(StreamTarget id, [MaybeNullWhen(false)] out QuicStreamState state);

    /// <summary>Re-arms the read pump for the given stream (no-op when torn down / detached).</summary>
    void RequestStreamRead(StreamTarget streamId);

    /// <summary>Handles a graceful inbound completion (EOF) for the given stream.</summary>
    void OnInboundComplete(DisconnectReason reason, long rawStreamId);

    /// <summary>
    /// Machine-specific routing of a non-graceful read failure (connection-level vs. per-stream). The
    /// QuicException-as-graceful classification is already handled by <see cref="QuicStreamReads"/>.
    /// </summary>
    void OnReadFailure(QuicStreamState state, Exception error);
}

/// <summary>
/// Single shared read-completion handler for both QUIC state machines. Collapses the two formerly
/// duplicated read-completion regions into one: null buffer maps to a graceful inbound completion, data
/// is pushed as <see cref="MultiplexedData"/> and the read pump is re-armed.
/// </summary>
internal static class QuicStreamReads
{
    public static void OnReceiveCompleted(IQuicStreamReadHost host, QuicStreamState state, WireBuffer? buffer, int epoch)
    {
        // Epoch guard (first, before touching any state/dict/host): the event was produced by a PipeTo
        // transform captured at a specific rent generation. QuicStreamState is pooled and always
        // re-rented after quiesce, so a completion for a now-dead stream can still be in the mailbox when
        // the object has been re-rented for a NEW stream — at which point its StreamId maps back to itself
        // and the membership/ReferenceEquals check below would wrongly pass. A mismatched epoch means this
        // read belongs to a previous rent: drop it and release its buffer before any side effect.
        if (epoch != state.Epoch)
        {
            buffer?.Dispose();
            return;
        }

        var streamId = StreamTarget.FromId(state.StreamId);

        // Identity guard replaces the old CompleteRead never-repool teardown check: if the stream was
        // torn down (removed from the machine, and possibly the state repooled and re-rented under a new
        // id), drop the stale completion and release any buffer it carried.
        if (!host.TryGetStream(streamId, out var current) || !ReferenceEquals(current, state))
        {
            buffer?.Dispose();
            return;
        }

        if (buffer is null)
        {
            host.OnInboundComplete(DisconnectReason.Graceful, state.StreamId);
            return;
        }

        host.Ops.OnPushInbound(MultiplexedData.Rent(buffer, streamId));
        host.RequestStreamRead(streamId);
    }

    public static void OnReceiveFailed(IQuicStreamReadHost host, QuicStreamState state, Exception error, int epoch)
    {
        // Epoch guard (first, before any host callback): an OperationCanceledException is produced on
        // EVERY quiesce-cancelled read, so a stale failure from a state that has since been repooled and
        // re-rented must never route to the host — otherwise it tears down the NEW stream with
        // DisconnectReason.Error. A mismatched epoch means this failure belongs to a previous rent.
        if (epoch != state.Epoch)
        {
            return;
        }

        // A QuicException on read means the peer closed or reset the stream (FIN/STOP_SENDING/
        // RST_STREAM) — a graceful stream completion, not an error to propagate. Deliberately NOT guarded
        // by stream membership: a connection-level failure must still tear the transport down even when the
        // faulting stream has already been removed (matches the pre-migration failure handlers).
        if (error is QuicException)
        {
            host.OnInboundComplete(DisconnectReason.Graceful, state.StreamId);
            return;
        }

        host.OnReadFailure(state, error);
    }
}
