namespace Servus.Akka.Transport;

public readonly record struct StreamTarget(long Value)
{
    public static StreamTarget FromId(long id) => new(id);

    public override string ToString() => Value.ToString();

    public static implicit operator StreamTarget(long value) => new(value);
    public static implicit operator StreamTarget(int value) => new(value);
    public static implicit operator long(StreamTarget target) => target.Value;
}