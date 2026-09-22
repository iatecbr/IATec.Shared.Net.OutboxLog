using CsCheck;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests asserting that writes to the outbox store are idempotent per
/// deduplication key: writing the same payload repeatedly yields exactly one stored entry,
/// with every write after the first reported as deduplicated and the stored entry left
/// unchanged.
/// </summary>
public class IdempotentWritePropertyTests
{
    /// <summary>
    /// Generates payload field values, including edge cases such as empty/whitespace strings,
    /// Unicode, control characters, and the ':' delimiter used by the canonical serialization.
    /// </summary>
    private static readonly Gen<string> GenField =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const(":"),
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

    /// <summary>Number of times the same payload is written: 2..5.</summary>
    private static readonly Gen<int> GenWriteCount = Gen.Int[2, 5];

    // Feature: logs-outbox-library, Property 8: Writes are idempotent per deduplication key
    // Validates: Requirements 3.7, 6.2
    [Fact]
    public void RepeatedWrites_AreIdempotent_PerDeduplicationKey()
    {
        Gen.Select(GenPayload, GenWriteCount)
            .Sample(testCase =>
            {
                var (payload, writeCount) = testCase;

                // Fresh store per iteration so the first write is genuinely a first-time write.
                var store = new InMemoryOutboxStore();

                // First write must persist a new entry.
                var first = store.WriteAsync(payload).GetAwaiter().GetResult();
                Assert.Equal(WriteOutcome.Persisted, first.Outcome);

                var key = first.DeduplicationKey;
                Assert.False(string.IsNullOrEmpty(key));

                // Capture the state of the originally stored entry immediately after the first write.
                var initialEntries = GetStoredEntries(store, key);
                var initialEntry = Assert.Single(initialEntries);
                var originalId = initialEntry.Id;
                Assert.Equal(DeliveryStatus.Pending, initialEntry.Status);
                Assert.Equal(0, initialEntry.AttemptCount);

                // Every subsequent write of the same payload must be deduplicated with the same key.
                for (var i = 1; i < writeCount; i++)
                {
                    var result = store.WriteAsync(payload).GetAwaiter().GetResult();
                    Assert.Equal(WriteOutcome.Deduplicated, result.Outcome);
                    Assert.Equal(key, result.DeduplicationKey);
                }

                // Exactly one entry is stored for that deduplication key.
                var entriesForKey = GetStoredEntries(store, key);
                var finalEntry = Assert.Single(entriesForKey);

                // The stored entry is unchanged across the repeated writes.
                Assert.Equal(originalId, finalEntry.Id);
                Assert.Equal(DeliveryStatus.Pending, finalEntry.Status);
                Assert.Equal(0, finalEntry.AttemptCount);
            }, iter: 100);
    }

    /// <summary>
    /// Returns every stored entry matching the supplied deduplication key. A freshly written
    /// entry is Pending with attempt count 0 and no backoff gate, so it is returned by the
    /// pending-batch query with a generous batch size and retry limit.
    /// </summary>
    private static IReadOnlyList<OutboxEntry> GetStoredEntries(InMemoryOutboxStore store, string key)
    {
        var pending = store.GetPendingAsync(batchSize: 10_000, retryLimit: int.MaxValue)
            .GetAwaiter().GetResult();
        return pending.Where(e => e.DeduplicationKey == key).ToList();
    }
}
