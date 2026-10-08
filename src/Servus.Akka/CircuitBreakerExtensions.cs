using Akka.Actor;
using Akka.Pattern;

namespace Servus.Akka;

public static class CircuitBreakerExtensions
{
    public static CircuitBreaker CreateCircuitBreaker(this IActorContext context,
        int maxFailures = 5,
        TimeSpan? callTimeout = null,
        TimeSpan? resetTimeout = null,
        Func<CircuitBreaker, CircuitBreaker>? configure = null)
    {
        var breaker = new CircuitBreaker(
            context.System.Scheduler,
            maxFailures,
            callTimeout ?? TimeSpan.FromSeconds(10),
            resetTimeout ?? TimeSpan.FromSeconds(30));

        return configure is not null ? configure(breaker) : breaker;
    }

    public static CircuitBreaker CreateCircuitBreaker(this ActorSystem system,
        int maxFailures = 5,
        TimeSpan? callTimeout = null,
        TimeSpan? resetTimeout = null,
        Func<CircuitBreaker, CircuitBreaker>? configure = null)
    {
        var breaker = new CircuitBreaker(
            system.Scheduler,
            maxFailures,
            callTimeout ?? TimeSpan.FromSeconds(10),
            resetTimeout ?? TimeSpan.FromSeconds(30));

        return configure is not null ? configure(breaker) : breaker;
    }
}
