using System.Diagnostics.Metrics;
using Servus.Diagnostics;

namespace Servus.Akka.Transport;

internal static class ServusMetricsExtensions
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