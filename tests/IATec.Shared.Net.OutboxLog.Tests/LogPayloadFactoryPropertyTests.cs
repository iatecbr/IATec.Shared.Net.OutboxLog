using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for <see cref="LogPayloadFactory"/>.
/// </summary>
public class LogPayloadFactoryPropertyTests
{
    /// <summary>
    /// Generates content lengths that span the truncation boundary. Includes a broad range
    /// (0..~10000) plus explicit values exactly at and adjacent to the 8192-character limit
    /// (8191, 8192, 8193) so the boundary is always exercised.
    /// </summary>
    private static readonly Gen<int> GenContentLength =
        Gen.OneOf(
            Gen.Int[0, 10_000],
            Gen.Const(LogPayloadFactory.MaxContentLength - 1),
            Gen.Const(LogPayloadFactory.MaxContentLength),
            Gen.Const(LogPayloadFactory.MaxContentLength + 1));

    // Feature: logs-outbox-library, Property 2: Content is truncated to 8192 characters
    // Validates: Requirements 2.3
    [Fact]
    public void ContentIsTruncatedTo8192Characters()
    {
        const int max = LogPayloadFactory.MaxContentLength;
        var factory = new LogPayloadFactory();

        GenContentLength.Sample(length =>
        {
            // A deterministic content string of the target length. Character values vary by
            // position so prefix equality is a meaningful check (not a run of identical chars).
            var original = BuildContent(length);

            // Drive the factory with a formatter that returns the generated content verbatim.
            var payload = factory.Create(
                category: "TestCategory",
                level: LogLevel.Information,
                eventId: new EventId(0),
                state: original,
                exception: null,
                formatter: static (s, _) => s,
                scopeProvider: null,
                options: new LogsOutboxOptions());

            // Resulting length is the original length clamped to the maximum.
            var expectedLength = System.Math.Min(length, max);
            Assert.Equal(expectedLength, payload.Content.Length);

            if (length > max)
            {
                // When truncated, the content is exactly the first 8192 characters of the original.
                Assert.Equal(original.Substring(0, max), payload.Content);
            }
            else
            {
                // Otherwise the content is preserved unchanged.
                Assert.Equal(original, payload.Content);
            }
        }, iter: 100);
    }

    private static string BuildContent(int length)
    {
        var buffer = new char[length];
        for (var i = 0; i < length; i++)
        {
            // Printable ASCII range 32..126, varying by position.
            buffer[i] = (char)(32 + (i % 95));
        }

        return new string(buffer);
    }

    // Feature: logs-outbox-library, Property 3: Missing required fields default to empty string
    // Validates: Requirements 2.4
    [Fact]
    public void MissingRequiredFields_DefaultToEmptyString_And_ConstructionDoesNotThrow()
    {
        var factory = new LogPayloadFactory();

        // Drive the factory with no source for any required field:
        //   - null scope provider (no scope values),
        //   - options with null ContainerKey/Source (no options context),
        //   - empty category (source falls back to category, so it must resolve to ""),
        //   - default EventId (EventId.Name is null, so action must resolve to ""),
        // while varying the log level and the formatter's returned content across iterations.
        Gen.Select(
                Gen.Int[0, 5],                       // maps to a LogLevel
                Gen.OneOf(                           // arbitrary formatter output
                    Gen.Const(""),
                    Gen.Const(" "),
                    Gen.Const("plain message"),
                    Gen.Const("héllo-\u00e9\u4e2d\u6587"),
                    Gen.String))
            .Sample(input =>
            {
                var (levelIndex, content) = input;
                var level = (LogLevel)levelIndex;

                var options = new LogsOutboxOptions
                {
                    ContainerKey = null,
                };

                LogPayload payload = factory.Create(
                    category: "",
                    level: level,
                    eventId: default,
                    state: content,
                    exception: null,
                    formatter: static (state, _) => state,
                    scopeProvider: null,
                    options: options);

                // Construction produced a payload (the Create call above would have thrown otherwise).
                Assert.NotNull(payload);

                // Required fields default to the empty string, never null.
                Assert.NotNull(payload.ContainerKey);
                Assert.Equal("", payload.ContainerKey);

                Assert.NotNull(payload.Source);
                Assert.Equal("", payload.Source);

                // owner falls back to the category; with an empty category it is "".
                Assert.NotNull(payload.Owner);
                Assert.Equal("", payload.Owner);

                // action falls back to EventId.Name and then to the log level; with a default
                // EventId (no name) it resolves to the level string (never "" for a real level).
                Assert.NotNull(payload.Action);
                Assert.Equal(level.ToString(), payload.Action);

                // userId falls back to options.UserIdProvider; with none configured it is "".
                Assert.NotNull(payload.UserId);
                Assert.Equal("", payload.UserId);
            }, iter: 100);
    }

    // Feature: logs-outbox-library, Property 3b: Missing owner/action/userId fall back to
    // category / log level / UserIdProvider respectively.
    // Validates: Requirements 2.4
    [Fact]
    public void MissingFields_FallBackTo_CategoryLevelAndUserIdProvider()
    {
        const string userId = "authenticated-user";
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var factory = new LogPayloadFactory(serviceProvider);

        Gen.Select(
                Gen.Int[0, 5],
                Gen.OneOf(Gen.Const("Category.Alpha"), Gen.Const("My.Namespace.Worker"), Gen.Const("X")))
            .Sample(input =>
            {
                var (levelIndex, category) = input;
                var level = (LogLevel)levelIndex;

                var options = new LogsOutboxOptions
                {
                    ContainerKey = null,
                    // The provider ignores the service provider here and returns a fixed user id.
                    UserIdProvider = _ => userId,
                };

                LogPayload payload = factory.Create(
                    category: category,
                    level: level,
                    eventId: default,
                    state: "message",
                    exception: null,
                    formatter: static (s, _) => s,
                    scopeProvider: null,
                    options: options);

                // owner falls back to the logging category (the context class).
                Assert.Equal(category, payload.Owner);

                // action falls back to the log level (no EventId.Name, no action scope).
                Assert.Equal(level.ToString(), payload.Action);

                // userId falls back to the configured provider.
                Assert.Equal(userId, payload.UserId);

                // source also falls back to the category (no source scope, no options.Source).
                Assert.Equal(category, payload.Source);
            }, iter: 100);
    }
}
