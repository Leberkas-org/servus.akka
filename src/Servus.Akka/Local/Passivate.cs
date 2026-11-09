namespace Servus.Akka.Local;

/// <summary>
/// Sent by an entity to its parent region to be stopped gracefully. The region stops delivering new messages to the
/// entity, sends <paramref name="StopMessage"/> to it and buffers messages for it until it terminated; then it starts
/// the entity again for the buffered messages. The entity stops itself when it receives the stop message.
/// </summary>
/// <param name="StopMessage">The message the entity stops itself on, after finishing its current work (for example
/// pending persist callbacks).</param>
public sealed record Passivate(object StopMessage);
