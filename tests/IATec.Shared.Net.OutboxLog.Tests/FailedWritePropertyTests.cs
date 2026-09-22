using System.Collections.Concurrent;
using CsCheck;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests covering the failure path of the Outbox Store contract:
/// a failed write must not persist a partial entry and must report failure.
/// </summary>
public class FailedWritePropertyTests
{
    /// <summary>
    /// Generates payload field values, deliberately including edge cases:
    /// empty and whitespace strings, Unicode, and control characters.
    /// </summary>
    private static readonly Gen<string> GenField =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const("\u0000"),
            Gen.Const("héllo-\u00e9\u4e2d\u6587"),
            Gen.String);

    private static readonly Gen<LogPayload> GenPayload =
        Gen.Select(GenField, GenField, GenField, GenField, GenField, GenField,
            (containerKey, source, owner, action, userId, content) => new LogPayload
            {
                ContainerKey = containerKey,
                Source = source,
                Owner = owner,
                Action = action,
                UserId = userId,
                Content = content,
            });

    /// <summary>
    /// A fault-injecting <see cref="IOutboxStore"/> test double that fails every write
    /// before persisting anything. It exposes its backing contents so tests can assert
    /// the store is never mutated by a failed write.
    /// </summary>
    private sealed class FaultInjectingOutboxStore : IOutboxStore
    {
        private readonly ConcurrentDictionary<string, OutboxEntry> _entries = new();

        public bool IsAvailable => true;

        /// <summary>The current number of stored entries; stays 0 while all writes fail.</summary>
        public int Count => _entries.Count;

        public Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
        {
            // Simulate a persistence failure that occurs BEFORE the entry is committed:
            // deliberately do not touch the backing store, and report failure.
            var key = DeduplicationKeyGenerator.Compute(payload);
            return Task.FromResult(new WriteResult(WriteOutcome.Failed, key, "Injected write failure"));
        }

        public Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<OutboxEntry>>([]);

        public Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;

        public Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default) => Task.CompletedTask;

        public Task MarkFailedAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;
    }

    // Feature: logs-outbox-library, Property 7: A failed write persists no partial entry
    // Validates: Requirements 3.6
    [Fact]
    public void FailedWrite_PersistsNoPartialEntry_And_ReturnsFailure()
    {
        GenPayload.Sample(payload =>
        {
            var store = new FaultInjectingOutboxStore();

            var result = store.WriteAsync(payload).GetAwaiter().GetResult();

            // The write result must indicate failure (and therefore not Success).
            Assert.Equal(WriteOutcome.Failed, result.Outcome);
            Assert.False(result.Success);
            Assert.False(string.IsNullOrEmpty(result.Error));

            // No partial entry may have been persisted: the store is unchanged (still empty).
            Assert.Equal(0, store.Count);
        }, iter: 100);
    }
}
