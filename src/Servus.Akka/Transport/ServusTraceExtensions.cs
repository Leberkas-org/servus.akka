using System.Diagnostics;
using Servus.Diagnostics;

namespace Servus.Akka.Transport;

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