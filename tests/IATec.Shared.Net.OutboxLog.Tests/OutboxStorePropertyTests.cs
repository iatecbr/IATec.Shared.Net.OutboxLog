using CsCheck;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for <see cref="IOutboxStore"/> implementations, exercised against the
/// reference <see cref="InMemoryOutboxStore"/>. This class holds Properties 6-9 from the
/// logs-outbox-library design. Each test uses a distinct, descriptive method name so that
/// properties added concurrently do not clash.
/// </summary>
public class OutboxStorePropertyTests
{
    /// <summary>
    /// Generates payload field values including edge cases: empty and whitespace strings,
    /// Unicode, control characters, and the ':' delimiter used by canonical serialization.
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

    // Feature: logs-outbox-library, Property 6: A successful write produces a valid pending entry
    // Validates: Requirements 3.1
    [Fact]
    public void SuccessfulWrite_ProducesValidPendingEntry()
    {
        GenPayload.Sample(payload =>
        {
            // Fresh store per iteration so each write is genuinely a first-time (Persisted) write.
            var store = new InMemoryOutboxStore();

            var result = store.WriteAsync(payload).GetAwaiter().GetResult();

            // The write must be persisted (not deduplicated/failed) with a non-empty dedup key.
            Assert.Equal(WriteOutcome.Persisted, result.Outcome);
            Assert.False(string.IsNullOrEmpty(result.DeduplicationKey));
            Assert.Null(result.Error);

            // Fetch pending entries with a large batch size and a high retry limit so the
            // just-written entry is guaranteed to be included.
            var pending = store.GetPendingAsync(batchSize: 10_000, retryLimit: int.MaxValue)
                .GetAwaiter().GetResult();

            var entry = Assert.Single(pending);

            // A successful write persists a pending entry with attempt count zero and a
            // non-empty deduplication key matching the write result.
            Assert.Equal(DeliveryStatus.Pending, entry.Status);
            Assert.Equal(0, entry.AttemptCount);
            Assert.False(string.IsNullOrEmpty(entry.DeduplicationKey));
            Assert.Equal(result.DeduplicationKey, entry.DeduplicationKey);
        }, iter: 100);
    }
}
