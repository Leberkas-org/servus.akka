namespace Servus.Akka.Transport;

/// <summary>
/// Adaptive receive-buffer sizing shared by the duplex connection types. Grows the rent hint when a
/// read fills most of the buffer and shrinks it after a streak of small reads, keeping idle
/// connections cheap while letting bulk transfers rent large buffers. Lifted verbatim from
/// <c>SocketPipeConnection.AdaptHint</c>.
/// </summary>
internal static class AdaptiveHint
{
    public const int MinHint = 4 * 1024;
    public const int MaxHint = 128 * 1024;
    private const int ShrinkStreakThreshold = 2;

    public static void Adapt(int bytesRead, ref int hint, ref int shrinkStreak)
    {
        if (bytesRead >= hint * 3 / 4)
        {
            shrinkStreak = 0;
            if (hint < MaxHint)
            {
                hint = Math.Min(hint * 2, MaxHint);
            }
        }
        else if (bytesRead < hint / 4)
        {
            if (++shrinkStreak >= ShrinkStreakThreshold && hint > MinHint)
            {
                hint = Math.Max(hint / 2, MinHint);
                shrinkStreak = 0;
            }
        }
        else
        {
            shrinkStreak = 0;
        }
    }
}
