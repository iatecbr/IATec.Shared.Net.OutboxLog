using System.Collections.Concurrent;
using System.Threading;
using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests covering the disposal behaviour of <see cref="OutboxLoggerProvider"/>:
/// once the provider is disposed it discards every subsequent log event without writing anything
/// to the store (Req 2.6).
/// </summary>
public class DisposedProviderPropertyTests
{
    /// <summary>
    /// Generates payload field values, including edge cases: empty and whitespace strings,
    /// Unicode and control characters, so the property holds across varied log inputs.
    /// </summary>
    private static readonly Gen<string> GenField =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const("\u0000"),
            Gen.Const("héllo-\u00e9\u4e2d\u6587"),
            Gen.String);

    /// <summary>Generates a log level (including None so guarded paths are exercised).</summary>
    private static readonly Gen<LogLevel> GenLevel =
        Gen.Int[0, 6].Select(static i => (LogLevel)i);

    /// <summary>Generates the number of log events emitted per iteration (always >= 1).</summary>
    private static readonly Gen<int> GenEventCount = Gen.Int[1, 8];

    /// <summary>
    /// A counting <see cref="IOutboxStore"/> test double. Every <see cref="WriteAsync"/> increments
    /// a thread-safe counter and records the payload, then reports success. Tests assert the counter
    /// stays at 0 after the owning provider is disposed.
    /// </summary>
    private sealed class CountingOutboxStore : IOutboxStore
    {
        private readonly ConcurrentQueue<LogPayload> _written = new();
        private int _writeCount;

        public bool IsAvailable => true;

        /// <summary>The total number of writes observed by the store.</summary>
        public int WriteCount => Volatile.Read(ref _writeCount);

        public Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _writeCount);
            _written.Enqueue(payload);
            var key = DeduplicationKeyGenerator.Compute(payload);
            return Task.FromResult(new WriteResult(WriteOutcome.Persisted, key, null));
        }

        public Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<OutboxEntry>>([]);

        public Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;

        public Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default) => Task.CompletedTask;

        public Task MarkFailedAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;
    }

    // Feature: logs-outbox-library, Property 5: A disposed provider discards subsequent events
    // Validates: Requirements 2.6
    [Fact]
    public void DisposedProvider_DiscardsSubsequentEvents_WritingNothing()
    {
        Gen.Select(GenEventCount, GenLevel, GenField, GenField)
            .Sample(input =>
            {
                var (eventCount, level, category, content) = input;

                var store = new CountingOutboxStore();
                var provider = new OutboxLoggerProvider(
                    store,
                    new LogPayloadFactory(),
                    new LogsOutboxOptions());

                // Obtain a logger BEFORE disposal, then dispose the provider.
                var loggerBeforeDisposal = provider.CreateLogger(category);
                provider.Dispose();

                // Emit events via the logger acquired before disposal.
                for (var i = 0; i < eventCount; i++)
                {
                    loggerBeforeDisposal.Log(
                        level == LogLevel.None ? LogLevel.Information : level,
                        new EventId(i),
                        content,
                        null,
                        static (s, _) => s);
                }

                // Also emit via a logger obtained AFTER disposal (CreateLogger still returns a logger
                // wired to the disposed provider, which must likewise discard events).
                var loggerAfterDisposal = provider.CreateLogger(category + "-post");
                for (var i = 0; i < eventCount; i++)
                {
                    loggerAfterDisposal.Log(
                        level == LogLevel.None ? LogLevel.Information : level,
                        new EventId(i),
                        content,
                        null,
                        static (s, _) => s);
                }

                // No entries may be written after disposal, regardless of input or logger source.
                Assert.Equal(0, store.WriteCount);
            }, iter: 100);
    }

    // Feature: logs-outbox-library, Property 5: A disposed provider discards subsequent events
    // Validates: Requirements 2.6
    [Fact]
    public void BeforeDisposal_WriteIncrementsStore_Sanity()
    {
        Gen.Select(GenEventCount, GenField, GenField)
            .Sample(input =>
            {
                var (eventCount, category, content) = input;

                var store = new CountingOutboxStore();
                var provider = new OutboxLoggerProvider(
                    store,
                    new LogPayloadFactory(),
                    new LogsOutboxOptions());

                var logger = provider.CreateLogger(category);

                // While the provider is alive, each emitted (enabled) event is written to the store.
                for (var i = 0; i < eventCount; i++)
                {
                    logger.Log(
                        LogLevel.Information,
                        new EventId(i),
                        content,
                        null,
                        static (s, _) => s);
                }

                Assert.Equal(eventCount, store.WriteCount);

                // After disposal, further events are discarded and the count no longer grows.
                provider.Dispose();
                var countAtDisposal = store.WriteCount;

                for (var i = 0; i < eventCount; i++)
                {
                    logger.Log(
                        LogLevel.Information,
                        new EventId(i),
                        content,
                        null,
                        static (s, _) => s);
                }

                Assert.Equal(countAtDisposal, store.WriteCount);
            }, iter: 100);
    }
}
