using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class StreamDirectionSpec
{
    [Fact(Timeout = 5000)]
    public void StreamDirection_should_have_two_values()
    {
        var values = Enum.GetValues<StreamDirection>();

        Assert.Equal(2, values.Length);
    }

    [Fact(Timeout = 5000)]
    public void StreamDirection_should_contain_Unidirectional()
    {
        Assert.True(Enum.IsDefined(StreamDirection.Unidirectional));
    }

    [Fact(Timeout = 5000)]
    public void StreamDirection_should_contain_Bidirectional()
    {
        Assert.True(Enum.IsDefined(StreamDirection.Bidirectional));
    }
}
