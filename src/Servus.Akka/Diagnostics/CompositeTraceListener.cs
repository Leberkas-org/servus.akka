using Servus.Diagnostics;

namespace Servus.Akka.Diagnostics;

/// <summary>
/// Fans a single Senf trace stream out to multiple listeners so components can attach and detach
/// diagnostic sinks without replacing whatever listener is already configured —
/// <see cref="ServusTrace.Configure"/> holds exactly one listener, so installing this composite as
/// that listener (typically once at startup, at <see cref="TraceLevel.Trace"/>) is what makes
/// additive registration possible. Each child carries its own minimum level, and its own
/// <see cref="IServusTraceListener.IsEnabled"/> is consulted per event, so a verbose ring-buffer
/// sink can coexist with an Info-level logger bridge without flooding it.
/// </summary>
public sealed class CompositeTraceListener : IServusTraceListener
{
    private readonly object _mutationLock = new();
    private Entry[] _entries = [];

    /// <summary>
    /// Registers a child listener that receives events at or above <paramref name="minimumLevel"/>.
    /// Returns a scope that removes the child again on dispose (idempotent), so callers can attach
    /// a diagnostic sink for the duration of a test or investigation.
    /// </summary>
    public IDisposable Add(IServusTraceListener listener, TraceLevel minimumLevel = TraceLevel.Trace)
    {
        ArgumentNullException.ThrowIfNull(listener);
        var entry = new Entry(listener, minimumLevel);
        lock (_mutationLock)
        {
            var entries = _entries;
            var next = new Entry[entries.Length + 1];
            Array.Copy(entries, next, entries.Length);
            next[^1] = entry;
            Volatile.Write(ref _entries, next);
        }

        return new RemovalScope(this, entry);
    }

    public bool IsEnabled(TraceLevel level, string category)
    {
        var entries = Volatile.Read(ref _entries);
        foreach (var entry in entries)
        {
            if (level >= entry.MinimumLevel && entry.Listener.IsEnabled(level, category))
            {
                return true;
            }
        }

        return false;
    }

    public void Write(in TraceEvent evt)
    {
        var entries = Volatile.Read(ref _entries);
        foreach (var entry in entries)
        {
            if (evt.Level >= entry.MinimumLevel && entry.Listener.IsEnabled(evt.Level, evt.Category))
            {
                entry.Listener.Write(in evt);
            }
        }
    }

    private void Remove(Entry entry)
    {
        lock (_mutationLock)
        {
            var entries = _entries;
            var index = Array.IndexOf(entries, entry);
            if (index < 0)
            {
                return;
            }

            var next = new Entry[entries.Length - 1];
            Array.Copy(entries, next, index);
            Array.Copy(entries, index + 1, next, index, entries.Length - index - 1);
            Volatile.Write(ref _entries, next);
        }
    }

    private sealed record Entry(IServusTraceListener Listener, TraceLevel MinimumLevel);

    private sealed class RemovalScope(CompositeTraceListener owner, Entry entry) : IDisposable
    {
        private Entry? _entry = entry;

        public void Dispose()
        {
            var toRemove = Interlocked.Exchange(ref _entry, null);
            if (toRemove is not null)
            {
                owner.Remove(toRemove);
            }
        }
    }
}
