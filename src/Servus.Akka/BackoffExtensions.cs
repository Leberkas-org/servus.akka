using Akka.Actor;
using Akka.Pattern;

namespace Servus.Akka;

public static class BackoffExtensions
{
    public static Props BackoffOnFailure(this Props childProps, string childName,
        TimeSpan? minBackoff = null, TimeSpan? maxBackoff = null, double randomFactor = 0.2,
        Func<BackoffOptions, BackoffOptions>? configure = null)
    {
        var options = Backoff.OnFailure(childProps, childName,
            minBackoff ?? TimeSpan.FromSeconds(3), maxBackoff ?? TimeSpan.FromSeconds(30), randomFactor, maxNrOfRetries: -1);
        if (configure is not null)
        {
            options = configure(options);
        }

        return BackoffSupervisor.Props(options);
    }

    public static Props BackoffOnStop(this Props childProps, string childName,
        TimeSpan? minBackoff = null, TimeSpan? maxBackoff = null, double randomFactor = 0.2,
        Func<BackoffOptions, BackoffOptions>? configure = null)
    {
        var options = Backoff.OnStop(childProps, childName,
            minBackoff ?? TimeSpan.FromSeconds(3), maxBackoff ?? TimeSpan.FromSeconds(30), randomFactor, maxNrOfRetries: -1);
        if (configure is not null)
        {
            options = configure(options);
        }

        return BackoffSupervisor.Props(options);
    }
}
