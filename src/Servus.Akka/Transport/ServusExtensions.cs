using System.Diagnostics;
using System.Diagnostics.Metrics;
using Servus.Core.Diagnostics;

namespace Servus.Akka.Transport;

internal static class ServusExtensions
{
    private static Histogram<double>? _dnsLookupDuration;
    private static Histogram<double>? _socketConnectDuration;

    public static Histogram<double> DnsLookupDuration(this ServusMetrics metrics)
    {
        return _dnsLookupDuration ??= metrics.Meter.CreateHistogram<double>(
            "dns.lookup.duration",
            unit: "s",
            description: "Duration of DNS lookups in seconds");
    }

    public static Histogram<double> SocketConnectDuration(this ServusMetrics metrics)
    {
        return _socketConnectDuration ??= metrics.Meter.CreateHistogram<double>(
            "network.socket.connect.duration",
            unit: "s",
            description: "Duration of socket connect operations in seconds");
    }
}

internal static class ServusTraceExtensions
{
    public static Activity? StartDnsLookup(this ServusTrace trace, string hostname)
    {
        if (!trace.Source.HasListeners())
        {
            return null;
        }

        var activity = trace.Source.StartActivity("dns.lookup", ActivityKind.Client);
        activity?.SetTag("dns.question.name", hostname);
        return activity;
    }

    public static void SetDnsAnswers(this ServusTrace _, Activity activity, string[] answers)
    {
        activity.SetTag("dns.answers", string.Join(",", answers));
        activity.SetTag("dns.answer.count", answers.Length);
    }

    public static Activity? StartSocketConnect(this ServusTrace trace, string address, int port, string transport, string networkType)
    {
        if (!trace.Source.HasListeners())
        {
            return null;
        }

        var activity = trace.Source.StartActivity("network.socket.connect", ActivityKind.Client);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag("network.peer.address", address);
        activity.SetTag("network.peer.port", port);
        activity.SetTag("network.transport", transport);
        activity.SetTag("network.type", networkType);
        return activity;
    }

    public static void SetError(this ServusTrace _, Activity activity, Exception exception)
    {
        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.SetTag("error.type", exception.GetType().FullName);
        activity.SetTag("exception.message", exception.Message);
    }
}
