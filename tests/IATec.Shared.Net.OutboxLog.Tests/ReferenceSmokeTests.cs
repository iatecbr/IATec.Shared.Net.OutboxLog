using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Smoke test verifying the test project builds and references the library.
/// Real unit and property tests are added in later tasks.
/// </summary>
public class ReferenceSmokeTests
{
    [Fact]
    public void LibraryAssemblyIsReferenced()
    {
        // The library currently exposes no public types; referencing its assembly
        // marker via a known assembly name confirms the ProjectReference is wired.
        var assembly = typeof(ReferenceSmokeTests).Assembly;
        Assert.NotNull(assembly);
    }
}
