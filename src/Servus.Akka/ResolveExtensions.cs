using Akka.Actor;
using Akka.DependencyInjection;

namespace Servus.Akka;

public static class ResolveExtensions
{
    private static IActorRef Resolve<TActor>(IActorRefFactory factory,
        DependencyResolver resolver, string? name, params object[] args)
        where TActor : ActorBase
    {
        var props = resolver.Props<TActor>(args);
        return factory.ActorOf(props, name);
    }

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context, string? name, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(context.System);
        return Resolve<TActor>(context, resolver, name, args);
    }

    public static IActorRef ResolveChildActor<TActor>(this IActorContext context, params object[] args)
        where TActor : ActorBase
        => ResolveChildActor<TActor>(context, null, args);

    public static IActorRef ResolveActor<TActor>(this IActorContext context, string? name, params object[] args)
        where TActor : ActorBase
        => context.System.ResolveActor<TActor>(name, args);

    public static IActorRef ResolveActor<TActor>(this IActorContext context, params object[] args)
        where TActor : ActorBase
        => context.System.ResolveActor<TActor>(args);

    public static IActorRef ResolveActor<TActor>(this ActorSystem system, params object[] args)
        where TActor : ActorBase
        => ResolveActor<TActor>(system, null, args);

    public static IActorRef ResolveActor<TActor>(this ActorSystem system, string? name, params object[] args)
        where TActor : ActorBase
    {
        var resolver = DependencyResolver.For(system);
        return Resolve<TActor>(system, resolver, name, args);
    }
}
