using CsCheck;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for the pending-batch query of <see cref="InMemoryOutboxStore"/>
/// (<see cref="IOutboxStore.GetPendingAsync"/>).
/// </summary>
public class PendingBatchQueryPropertyTests
{
    /// <summary>
    /// A minimal, mutable <see cref="TimeProvider"/> so that <see cref="OutboxEntry.CreatedAt"/>
    /// and backoff comparisons are fully deterministic. Advancing the clock between writes gives
    /// each entry a distinct creation time, which lets us assert oldest-to-newest ordering.
    /// </summary>
    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        public MutableTimeProvider(DateTimeOffset start) => _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;

        public DateTimeOffset Now => _now;
    }

    /// <summary>
    /// The lifecycle transition to apply to an entry after it is written, used to build varied
    /// store states covering every status and the future-backoff exclusion case.
    /// </summary>
    private enum Transition
    {
        /// <summary>Leave the entry Pending with zero attempts.</summary>
        None,

        /// <summary>Mark the entry Delivered (must be excluded from pending batches).</summary>
        Deliver,

        /// <summary>Mark the entry Failed (must be excluded from pending batches).</summary>
        Fail,

        /// <summary>Increment attempts once with a backoff whose NextAttemptAt is already elapsed.</summary>
        IncrementElapsed,

        /// <summary>Increment attempts once with a backoff whose NextAttemptAt is in the future.</summary>
        IncrementFuture,
    }

    private static readonly Gen<Transition> GenTransition =
        Gen.OneOfConst(
            Transition.None,
            Transition.Deliver,
            Transition.Fail,
            Transition.IncrementElapsed,
            Transition.IncrementFuture);

    /// <summary>
    /// A single generated entry spec: a unique-ish payload seed (so each write is a distinct
    /// deduplication key), the transition to apply, and how many extra attempt increments to
    /// stack on top so we can exercise the attempts &lt; retryLimit boundary.
    /// </summary>
    private readonly record struct EntrySpec(int Seed, Transition Transition, int ExtraIncrements);

    private static readonly Gen<EntrySpec> GenEntrySpec =
        Gen.Select(
            Gen.Int[0, 1_000_000],
            GenTransition,
            Gen.Int[0, 12],
            (seed, transition, extra) => new EntrySpec(seed, transition, extra));

    // Feature: logs-outbox-library, Property 9: Pending-batch query invariant
    // Validates: Requirements 3.5, 5.5, 6.4
    [Fact]
    public void PendingBatch_SatisfiesQueryInvariant()
    {
        // Generate: a set of entry specs (the store state), a retry limit in the configurable
        // range [1, 10], and a batch size in the configurable range [1, 1000].
        var gen = Gen.Select(
            GenEntrySpec.Array[0, 30],
            Gen.Int[1, 10],
            Gen.Int[1, 1000]);

        gen.Sample(t =>
        {
            var (specs, retryLimit, batchSize) = t;

            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new MutableTimeProvider(start);
            var store = new InMemoryOutboxStore(clock);

            // Track the CreatedAt assigned to each successfully-persisted entry so we can verify
            // ordering independently of the store's internal bookkeeping.
            var createdAtById = new Dictionary<Guid, DateTimeOffset>();
            var deliveredOrFailedIds = new HashSet<Guid>();

            for (var i = 0; i < specs.Length; i++)
            {
                var spec = specs[i];

                // Advance the clock before each write so every entry gets a strictly increasing,
                // distinct CreatedAt. This makes the oldest-to-newest assertion unambiguous.
                clock.Advance(TimeSpan.FromSeconds(1));
                var createdAt = clock.Now;

                // Use the loop index in the payload to guarantee a unique deduplication key per
                // write (the seed alone could collide across specs).
                var payload = new LogPayload
                {
                    ContainerKey = "c",
                    Source = "s",
                    Owner = "o",
                    Action = "a",
                    UserId = "u",
                    Content = $"entry-{i}-{spec.Seed}",
                };

                var write = store.WriteAsync(payload).GetAwaiter().GetResult();
                Assert.Equal(WriteOutcome.Persisted, write.Outcome);

                // Recover the entry id by reading it back while it is still the newest pending
                // entry (attempts 0, status Pending, no future backoff). We query with a generous
                // batch/retry to find it, then match on the unique content.
                var justWritten = store
                    .GetPendingAsync(int.MaxValue, int.MaxValue)
                    .GetAwaiter().GetResult()
                    .Single(e => e.Payload.Content == payload.Content);

                createdAtById[justWritten.Id] = createdAt;

                switch (spec.Transition)
                {
                    case Transition.None:
                        break;

                    case Transition.Deliver:
                        store.MarkDeliveredAsync(justWritten.Id).GetAwaiter().GetResult();
                        deliveredOrFailedIds.Add(justWritten.Id);
                        break;

                    case Transition.Fail:
                        store.MarkFailedAsync(justWritten.Id).GetAwaiter().GetResult();
                        deliveredOrFailedIds.Add(justWritten.Id);
                        break;

                    case Transition.IncrementElapsed:
                        {
                            var count = 1 + spec.ExtraIncrements;
                            for (var k = 0; k < count; k++)
                            {
                                // NextAttemptAt in the past → not gated by backoff.
                                store.IncrementAttemptAsync(justWritten.Id, clock.Now - TimeSpan.FromMinutes(1))
                                    .GetAwaiter().GetResult();
                            }
                        }
                        break;

                    case Transition.IncrementFuture:
                        {
                            var count = 1 + spec.ExtraIncrements;
                            for (var k = 0; k < count; k++)
                            {
                                // NextAttemptAt in the future → excluded until backoff elapses.
                                store.IncrementAttemptAsync(justWritten.Id, clock.Now + TimeSpan.FromHours(1))
                                    .GetAwaiter().GetResult();
                            }
                        }
                        break;
                }
            }

            var result = store.GetPendingAsync(batchSize, retryLimit).GetAwaiter().GetResult();

            // (a) The batch never exceeds the requested size cap.
            Assert.True(result.Count <= batchSize, $"count {result.Count} exceeded batchSize {batchSize}");

            foreach (var entry in result)
            {
                // (b) Every returned entry has status Pending.
                Assert.Equal(DeliveryStatus.Pending, entry.Status);

                // (c) Every returned entry has attempts strictly below the retry limit.
                Assert.True(entry.AttemptCount < retryLimit,
                    $"attempts {entry.AttemptCount} not below retryLimit {retryLimit}");

                // (d) No Delivered/Failed entry is ever returned.
                Assert.DoesNotContain(entry.Id, deliveredOrFailedIds);

                // Entries whose NextAttemptAt is in the future are excluded.
                if (entry.NextAttemptAt is { } next)
                {
                    Assert.True(next <= clock.Now,
                        $"entry with future NextAttemptAt {next} (now {clock.Now}) was returned");
                }
            }

            // (e) Results are ordered oldest-to-newest by CreatedAt.
            for (var i = 1; i < result.Count; i++)
            {
                var prev = createdAtById[result[i - 1].Id];
                var curr = createdAtById[result[i].Id];
                Assert.True(prev <= curr,
                    $"ordering violated: {prev} came before {curr} in the result");
            }
        }, iter: 100);
    }
}
