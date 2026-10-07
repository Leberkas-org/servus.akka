using Akka.Actor;
using Akka.DependencyInjection;

namespace Servus.Akka;

public static class ResolveExtensions
{
    private static IActorRef Resolve<TActor>(IActorRefFactory factory,
        DependencyResolver resolver, string? name, Func<Props, Props>? configure, params object[] args)
        where TActor : ActorBase
    {
        var props = resolver.Props<TActor>(args);
        if (configure is not null)
        {
            props = configure(props);
        }

        return factory.ActorOf(props, name);
    }

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context, string? name, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(context.System);
        return Resolve<TActor>(context, resolver, name, configure: null, args);
    }

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context, params object[] args)
        where TActor : ActorBase
        => context.ResolveChildActor<TActor>((string?)null, args);

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context, string? name,
        Func<Props, Props> configure, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(context.System);
        return Resolve<TActor>(context, resolver, name, configure, args);
    }

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context,
        Func<Props, Props> configure, params object[] args)
        where TActor : ActorBase
        => context.ResolveChildActor<TActor>(null, configure, args);

    public static IActorRef ResolveActor<TActor>(this IActorContext context, string? name, params object[] args)
        where TActor : ActorBase
        => context.System.ResolveActor<TActor>(name, args);

    public static IActorRef ResolveActor<TActor>(this IActorContext context, params object[] args)
        where TActor : ActorBase
        => context.System.ResolveActor<TActor>(args);

    public static IActorRef ResolveActor<TActor>(this ActorSystem system, params object[] args)
        where TActor : ActorBase
        => system.ResolveActor<TActor>((string?)null, args);

    public static IActorRef ResolveActor<TActor>(this ActorSystem system, string? name, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(system);
        return Resolve<TActor>(system, resolver, name, configure: null, args);
    }

    public static IActorRef ResolveActor<TActor>(this ActorSystem system, string? name,
        Func<Props, Props> configure, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(system);
        return Resolve<TActor>(system, resolver, name, configure, args);
    }

    public static IActorRef ResolveActor<TActor>(this ActorSystem system,
        Func<Props, Props> configure, params object[] args)
        where TActor : ActorBase
        => ResolveActor<TActor>(system, null, configure, args);
}
