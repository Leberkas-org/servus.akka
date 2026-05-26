using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class PipeModeSpec
{
    [Fact(Timeout = 5000)]
    public void PipeMode_should_have_three_values()
    {
        var values = Enum.GetValues<PipeMode>();
        Assert.Equal(3, values.Length);
    }

    [Theory(Timeout = 5000)]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    public void PipeMode_should_have_correct_ordinal(int modeValue, int expected)
    {
        var mode = (PipeMode)modeValue;
        Assert.Equal(expected, (int)mode);
    }
}
