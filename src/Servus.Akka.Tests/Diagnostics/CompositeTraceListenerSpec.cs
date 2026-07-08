using Servus.Akka.Diagnostics;
using Servus.Diagnostics;
using static Servus.Senf;

namespace Servus.Akka.Tests.Diagnostics;

/// <summary>
/// Events are driven through the global <see cref="Servus.Senf.Tracing"/> API because
/// <see cref="TraceEvent"/> has an internal constructor. Every test uses a unique category so
/// parallel test collections that also trace cannot bleed into the assertions, and restores the
/// global tracer in a finally block.
/// </summary>
[Collection("Tracing")]
public sealed class CompositeTraceListenerSpec
{
    [Fact(Timeout = 5000)]
    public void IsEnabled_should_return_false_when_no_children_registered()
    {
        var composite = new CompositeTraceListener();

        Assert.False(composite.IsEnabled(TraceLevel.Error, "AnyCategory"));
    }

    [Fact(Timeout = 5000)]
    public void IsEnabled_should_return_true_when_any_child_accepts()
    {
        var composite = new CompositeTraceListener();
        composite.Add(new RecordingListener(), TraceLevel.Warning);
        composite.Add(new RecordingListener(), TraceLevel.Trace);

        Assert.True(composite.IsEnabled(TraceLevel.Trace, "AnyCategory"));
    }

    [Fact(Timeout = 5000)]
    public void IsEnabled_should_return_false_when_all_children_are_below_their_minimum_level()
    {
        var composite = new CompositeTraceListener();
        composite.Add(new RecordingListener(), TraceLevel.Warning);
        composite.Add(new RecordingListener(), TraceLevel.Info);

        Assert.False(composite.IsEnabled(TraceLevel.Debug, "AnyCategory"));
    }

    [Fact(Timeout = 5000)]
    public void IsEnabled_should_honor_child_category_filter()
    {
        var composite = new CompositeTraceListener();
        composite.Add(new RecordingListener(category => category == "Wanted"));

        Assert.True(composite.IsEnabled(TraceLevel.Trace, "Wanted"));
        Assert.False(composite.IsEnabled(TraceLevel.Trace, "Unwanted"));
    }

    [Fact(Timeout = 5000)]
    public void Write_should_fan_out_to_all_enabled_children()
    {
        var category = UniqueCategory();
        var first = new RecordingListener();
        var second = new RecordingListener();
        var composite = new CompositeTraceListener();
        composite.Add(first);
        composite.Add(second);

        WithGlobalTracer(composite, () =>
        {
            Tracing.Trace(this, TraceLevel.Info, category, "hello");
        });

        Assert.Single(first.Events, e => e.Category == category);
        Assert.Single(second.Events, e => e.Category == category);
    }

    [Fact(Timeout = 5000)]
    public void Write_should_skip_children_below_their_minimum_level()
    {
        var category = UniqueCategory();
        var verbose = new RecordingListener();
        var warningsOnly = new RecordingListener();
        var composite = new CompositeTraceListener();
        composite.Add(verbose, TraceLevel.Trace);
        composite.Add(warningsOnly, TraceLevel.Warning);

        WithGlobalTracer(composite, () =>
        {
            Tracing.Trace(this, TraceLevel.Debug, category, "debug detail");
            Tracing.Trace(this, TraceLevel.Warning, category, "warning");
        });

        Assert.Equal(2, verbose.Events.Count(e => e.Category == category));
        var received = Assert.Single(warningsOnly.Events, e => e.Category == category);
        Assert.Equal(TraceLevel.Warning, received.Level);
    }

    [Fact(Timeout = 5000)]
    public void Write_should_skip_children_whose_own_IsEnabled_rejects_the_category()
    {
        var category = UniqueCategory();
        var selective = new RecordingListener(c => c == "SomethingElse");
        var composite = new CompositeTraceListener();
        composite.Add(selective);
        composite.Add(new RecordingListener());

        WithGlobalTracer(composite, () =>
        {
            Tracing.Trace(this, TraceLevel.Info, category, "message");
        });

        Assert.DoesNotContain(selective.Events, e => e.Category == category);
    }

    [Fact(Timeout = 5000)]
    public void Add_should_return_scope_that_removes_the_child_on_dispose()
    {
        var category = UniqueCategory();
        var listener = new RecordingListener();
        var composite = new CompositeTraceListener();
        var scope = composite.Add(listener);

        WithGlobalTracer(composite, () =>
        {
            Tracing.Trace(this, TraceLevel.Info, category, "before removal");
            scope.Dispose();
            Tracing.Trace(this, TraceLevel.Info, category, "after removal");
        });

        var received = Assert.Single(listener.Events, e => e.Category == category);
        Assert.Equal("before removal", received.Message);
    }

    [Fact(Timeout = 5000)]
    public void Add_scope_dispose_should_be_idempotent()
    {
        var composite = new CompositeTraceListener();
        var keeper = new RecordingListener();
        var scope = composite.Add(new RecordingListener());
        composite.Add(keeper);

        scope.Dispose();
        scope.Dispose();

        Assert.True(composite.IsEnabled(TraceLevel.Trace, "AnyCategory"));
    }

    [Fact(Timeout = 5000)]
    public async Task Write_should_tolerate_concurrent_add_and_remove()
    {
        var category = UniqueCategory();
        var stable = new RecordingListener();
        var composite = new CompositeTraceListener();
        composite.Add(stable);

        using var cts = new CancellationTokenSource();
        var churn = Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
            {
                using var scope = composite.Add(new RecordingListener());
            }
        }, TestContext.Current.CancellationToken);

        WithGlobalTracer(composite, () =>
        {
            for (var i = 0; i < 1000; i++)
            {
                Tracing.Trace(this, TraceLevel.Info, category, "tick {0}", null, i);
            }
        });

        cts.Cancel();
        await churn;

        Assert.Equal(1000, stable.Events.Count(e => e.Category == category));
    }

    private static string UniqueCategory() => $"CompositeSpec-{Guid.NewGuid():N}";

    private static void WithGlobalTracer(IServusTraceListener listener, Action action)
    {
        Tracing.Configure(listener, TraceLevel.Trace);
        try
        {
            action();
        }
        finally
        {
            Tracing.Disable();
        }
    }

    private sealed class RecordingListener : IServusTraceListener
    {
        private readonly Func<string, bool> _categoryFilter;
        private readonly List<RecordedEvent> _events = [];

        public RecordingListener(Func<string, bool>? categoryFilter = null)
        {
            _categoryFilter = categoryFilter ?? (_ => true);
        }

        public IReadOnlyList<RecordedEvent> Events
        {
            get
            {
                lock (_events)
                {
                    return _events.ToArray();
                }
            }
        }

        public bool IsEnabled(TraceLevel level, string category) => _categoryFilter(category);

        public void Write(in TraceEvent evt)
        {
            var recorded = new RecordedEvent(evt.Level, evt.Category, evt.FormatMessage());
            lock (_events)
            {
                _events.Add(recorded);
            }
        }
    }

    private sealed record RecordedEvent(TraceLevel Level, string Category, string Message);
}
