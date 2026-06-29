using Akka.Actor;
using Akka.Hosting;

namespace Servus.Akka;

public static class RegistryExtensions
{
    public static IActorRef GetActor<T>(this IActorContext context) => context.System.GetActor<T>();
    public static IActorRef GetActor<T>(this ActorSystem system) => system.GetRegistry().Get<T>();

    public static bool TryGetActor<T>(this IActorContext context, out IActorRef actor) => context.System.TryGetActor<T>(out actor);
    public static bool TryGetActor<T>(this ActorSystem system, out IActorRef actor) => system.GetRegistry().TryGet<T>(out actor);

    public static bool TryGetActor(this IActorContext context, Type key, out IActorRef actor) => context.System.TryGetActor(key, out actor);
    public static bool TryGetActor(this ActorSystem system, Type key, out IActorRef actor) => system.GetRegistry().TryGet(key, out actor);

    public static Task<IActorRef> GetActorAsync<T>(this IActorContext context) => context.System.GetActorAsync<T>();
    public static Task<IActorRef> GetActorAsync<T>(this ActorSystem system) => system.GetRegistry().GetAsync<T>();

    public static IReadOnlyActorRegistry GetRegistry(this ActorSystem system) => ActorRegistry.For(system);
}