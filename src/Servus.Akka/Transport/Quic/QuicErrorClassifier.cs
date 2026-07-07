using System.Net.Quic;

namespace Servus.Akka.Transport.Quic;

/// <summary>
/// Shared classification of QUIC failures into connection-level (terminal — the whole connection is
/// gone) versus transient (recoverable, worth retrying). Used by both the client state machine's
/// read-failure handling and the listener's accept-loop delegate, so both sides draw the line in the
/// same place.
/// </summary>
internal static class QuicErrorClassifier
{
    internal static bool IsConnectionLevel(Exception ex)
    {
        if (ex is QuicException qe)
        {
            return qe.QuicError is QuicError.ConnectionAborted
                or QuicError.ConnectionIdle
                or QuicError.ConnectionRefused
                or QuicError.ConnectionTimeout;
        }

        return ex is ObjectDisposedException;
    }
}
