using System.Diagnostics;
using System.Diagnostics.Metrics;
using Servus.Akka.Transport;
using Servus.Core.Diagnostics;

namespace Servus.Akka.Tests.Transport;

public sealed class ServusExtensionsSpec
{
    [Fact(Timeout = 5000)]
    public void DnsLookupDuration_should_create_histogram_on_first_call()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram1 = metrics.DnsLookupDuration();

        Assert.NotNull(histogram1);
        Assert.Equal("dns.lookup.duration", histogram1.Name);
    }

    [Fact(Timeout = 5000)]
    public void DnsLookupDuration_should_return_cached_histogram_on_second_call()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram1 = metrics.DnsLookupDuration();
        var histogram2 = metrics.DnsLookupDuration();

        Assert.Same(histogram1, histogram2);
    }

    [Fact(Timeout = 5000)]
    public void DnsLookupDuration_should_create_histogram_with_correct_unit()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram = metrics.DnsLookupDuration();

        Assert.Equal("s", histogram.Unit);
    }

    [Fact(Timeout = 5000)]
    public void DnsLookupDuration_should_create_histogram_with_description()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram = metrics.DnsLookupDuration();

        Assert.Equal("Duration of DNS lookups in seconds", histogram.Description);
    }

    [Fact(Timeout = 5000)]
    public void SocketConnectDuration_should_create_histogram_on_first_call()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram1 = metrics.SocketConnectDuration();

        Assert.NotNull(histogram1);
        Assert.Equal("network.socket.connect.duration", histogram1.Name);
    }

    [Fact(Timeout = 5000)]
    public void SocketConnectDuration_should_return_cached_histogram_on_second_call()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram1 = metrics.SocketConnectDuration();
        var histogram2 = metrics.SocketConnectDuration();

        Assert.Same(histogram1, histogram2);
    }

    [Fact(Timeout = 5000)]
    public void SocketConnectDuration_should_create_histogram_with_correct_unit()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram = metrics.SocketConnectDuration();

        Assert.Equal("s", histogram.Unit);
    }

    [Fact(Timeout = 5000)]
    public void SocketConnectDuration_should_create_histogram_with_description()
    {
        var meter = new Meter("test-meter");
        var metrics = CreateServusMetrics(meter);

        var histogram = metrics.SocketConnectDuration();

        Assert.Equal("Duration of socket connect operations in seconds", histogram.Description);
    }

    [Fact(Timeout = 5000)]
    public void StartDnsLookup_should_return_null_when_no_listeners()
    {
        var source = new ActivitySource("test-source");
        var trace = CreateServusTrace(source);

        var activity = trace.StartDnsLookup("example.com");

        Assert.Null(activity);
    }

    [Fact(Timeout = 5000)]
    public void StartDnsLookup_should_return_activity_when_listeners_exist()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartDnsLookup("example.com");

        Assert.NotNull(activity);
        Assert.Equal("dns.lookup", activity.OperationName);
        Assert.Equal(ActivityKind.Client, activity.Kind);
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartDnsLookup_should_set_hostname_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartDnsLookup("example.com");

        Assert.NotNull(activity);
        Assert.Equal("example.com", activity.GetTagItem("dns.question.name"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartDnsLookup_should_set_different_hostnames()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity1 = trace.StartDnsLookup("example.com");
        var activity2 = trace.StartDnsLookup("test.org");

        Assert.Equal("example.com", activity1?.GetTagItem("dns.question.name"));
        Assert.Equal("test.org", activity2?.GetTagItem("dns.question.name"));
        activity1?.Dispose();
        activity2?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetDnsAnswers_should_set_answers_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var answers = new[] { "192.0.2.1", "192.0.2.2" };
        trace.SetDnsAnswers(activity, answers);

        Assert.Equal("192.0.2.1,192.0.2.2", activity.GetTagItem("dns.answers"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetDnsAnswers_should_set_answer_count_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var answers = new[] { "192.0.2.1", "192.0.2.2", "192.0.2.3" };
        trace.SetDnsAnswers(activity, answers);

        Assert.Equal(3, activity.GetTagItem("dns.answer.count"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetDnsAnswers_should_handle_single_answer()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var answers = new[] { "192.0.2.1" };
        trace.SetDnsAnswers(activity, answers);

        Assert.Equal("192.0.2.1", activity.GetTagItem("dns.answers"));
        Assert.Equal(1, activity.GetTagItem("dns.answer.count"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetDnsAnswers_should_handle_empty_answers()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var answers = Array.Empty<string>();
        trace.SetDnsAnswers(activity, answers);

        Assert.Equal(string.Empty, activity.GetTagItem("dns.answers"));
        Assert.Equal(0, activity.GetTagItem("dns.answer.count"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_return_null_when_no_listeners()
    {
        var source = new ActivitySource("test-source");
        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 80, "tcp", "ipv4");

        Assert.Null(activity);
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_return_activity_when_listeners_exist()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.NotNull(activity);
        Assert.Equal("network.socket.connect", activity.OperationName);
        Assert.Equal(ActivityKind.Client, activity.Kind);
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_set_address_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.Equal("192.0.2.1", activity?.GetTagItem("network.peer.address"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_set_port_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.Equal(8080, activity?.GetTagItem("network.peer.port"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_set_transport_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.Equal("tcp", activity?.GetTagItem("network.transport"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_set_network_type_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.Equal("ipv4", activity?.GetTagItem("network.type"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_handle_ipv6()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("::1", 443, "tcp", "ipv6");

        Assert.Equal("::1", activity?.GetTagItem("network.peer.address"));
        Assert.Equal(443, activity?.GetTagItem("network.peer.port"));
        Assert.Equal("ipv6", activity?.GetTagItem("network.type"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_handle_hostname()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("example.com", 80, "tcp", "ipv4");

        Assert.Equal("example.com", activity?.GetTagItem("network.peer.address"));
        activity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_handle_different_transports()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var tcpActivity = trace.StartSocketConnect("192.0.2.1", 80, "tcp", "ipv4");
        var udpActivity = trace.StartSocketConnect("192.0.2.1", 53, "udp", "ipv4");

        Assert.Equal("tcp", tcpActivity?.GetTagItem("network.transport"));
        Assert.Equal("udp", udpActivity?.GetTagItem("network.transport"));
        tcpActivity?.Dispose();
        udpActivity?.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartSocketConnect_should_return_null_when_source_starts_activity_returns_null()
    {
        var source = new ActivitySource("test-source");
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        };
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);

        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");

        Assert.Null(activity);
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_set_error_status()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var exception = new InvalidOperationException("Test error");
        trace.SetError(activity, exception);

        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_set_error_status_description()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var exception = new InvalidOperationException("Test error message");
        trace.SetError(activity, exception);

        Assert.Equal("Test error message", activity.StatusDescription);
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_set_error_type_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var exception = new InvalidOperationException("Test error");
        trace.SetError(activity, exception);

        Assert.Equal(typeof(InvalidOperationException).FullName, activity.GetTagItem("error.type"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_set_exception_message_tag()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var exception = new InvalidOperationException("Detailed error message");
        trace.SetError(activity, exception);

        Assert.Equal("Detailed error message", activity.GetTagItem("exception.message"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_handle_different_exception_types()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartDnsLookup("example.com");
        Assert.NotNull(activity);

        var exception = new ArgumentNullException(nameof(activity), "Parameter is null");
        trace.SetError(activity, exception);

        Assert.Equal(typeof(ArgumentNullException).FullName, activity.GetTagItem("error.type"));
        activity.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void SetError_should_work_on_socket_connect_activity()
    {
        var source = new ActivitySource("test-source");
        using var listener = CreateActivityListener();
        ActivitySource.AddActivityListener(listener);

        var trace = CreateServusTrace(source);
        var activity = trace.StartSocketConnect("192.0.2.1", 8080, "tcp", "ipv4");
        Assert.NotNull(activity);

        var exception = new IOException("Connection refused");
        trace.SetError(activity, exception);

        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("Connection refused", activity.StatusDescription);
        Assert.Equal(typeof(IOException).FullName, activity.GetTagItem("error.type"));
        activity.Dispose();
    }

    private static ServusMetrics CreateServusMetrics(Meter _)
    {
        return (ServusMetrics)Activator.CreateInstance(
            typeof(ServusMetrics),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
            null, null, null)!;
    }

    private static ServusTrace CreateServusTrace(ActivitySource _)
    {
        return (ServusTrace)Activator.CreateInstance(
            typeof(ServusTrace),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
            null, null, null)!;
    }

    private static ActivityListener CreateActivityListener()
    {
        return new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
    }
}
