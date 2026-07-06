using System.IO;
using System.Net.Sockets;

namespace Servus.Akka.Transport;

/// <summary>
/// A full-duplex byte connection with owned-buffer inbound and channel-based outbound. Inbound is
/// probe-gated rent-and-receive (<see cref="ReceiveAsync"/>); outbound is fire-and-forget enqueue
/// (<see cref="TryEnqueue"/>) drained by an internal send loop. <see cref="QuiesceAsync"/> parks the
/// inbound side without tearing the connection down so it can be reused.
/// </summary>
internal interface IDuplexConnection : IAsyncDisposable
{
    /// <summary>
    /// Fires on the send-loop thread after each drained batch is fully sent and its buffers disposed,
    /// with the total byte count of the batch. Set once per connection before enqueueing.
    /// </summary>
    Action<int>? OnFlushed { get; set; }

    /// <summary>
    /// Receives once into a freshly rented, caller-owned <see cref="WireBuffer"/>. Returns null on EOF,
    /// throws on error. At most one call may be outstanding at a time.
    /// </summary>
    ValueTask<WireBuffer?> ReceiveAsync();

    /// <summary>
    /// Enqueues a buffer for sending; ownership transfers to the connection. Returns false only after
    /// the output side has been completed/disposed — in that case the buffer was NOT consumed and the
    /// caller retains ownership.
    /// </summary>
    bool TryEnqueue(WireBuffer buffer);

    /// <summary>
    /// Cancels a pending receive and awaits its settlement. Returns true when the inbound side parked
    /// cleanly (reusable — the next <see cref="ReceiveAsync"/> works); false when data/EOF/error
    /// surfaced during the race (the caller should dispose the connection).
    /// </summary>
    ValueTask<bool> QuiesceAsync();

    /// <summary>Signals no further enqueues, drains the outbound channel, and finishes the send loop.</summary>
    Task CompleteAndDrainOutputAsync();
}

/// <summary>Shared classification of exceptions that are normal during connection teardown.</summary>
internal static class ConnectionErrors
{
    public static bool IsTeardown(Exception ex) =>
        ex is OperationCanceledException or SocketException or IOException or ObjectDisposedException;
}
