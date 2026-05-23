namespace Servus.Akka.Local;

public class LocalEntityRegionOptions
{
    public TimeSpan? PassivateIdleEntityAfter { get; set; }
    public IEntityIdStore? EntityIdStore { get; set; }
}
