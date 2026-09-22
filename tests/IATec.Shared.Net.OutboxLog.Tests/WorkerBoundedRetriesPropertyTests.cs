using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests asserting that the <see cref="DispatchWorker"/> bounds delivery retries by
/// the configured <see cref="LogsOutboxOptions.RetryLimit"/>: an entry that never gets accepted is
/// attempted at most <c>RetryLimit</c> times, transitions to <see cref="DeliveryStatus.Failed"/>
/// once the limit is reached, and is then excluded from every subsequent pending batch.
/// </summary>
public class WorkerBoundedRetriesPropertyTests
{
    /// <summary>
    /// A fake <see cref="ILogBankClient"/> that never accepts a payload (always returns
    /// <see cref="DeliveryOutcome.Unreachable"/>) and records how many times it was invoked so the
    /// test can assert the number of delivery attempts never exceeds the retry limit.
    /// </summary>
    private sealed class AlwaysUnreachableClient : ILogBankClient
    {
        private int _sendCount;

        public int SendCount => Volatile.Read(ref _sendCount);

        public Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
        {
            Interlocked.Increment(ref _sendCount);
            return Task.FromResult(new DeliveryResult(DeliveryOutcome.Unreachable, null));
        }
    }

    // Feature: logs-outbox-library, Property 13: Retries are bounded by the retry limit
    // Validates: Requirements 5.6, 8.5
    [Fact]
    public void Worker_BoundsRetriesByLimit_AndReachesFailed()
    {
        // Generate: a retry limit in the configurable range [1, 10] and a payload content seed so
        // each iteration exercises a distinct entry.
        var gen = Gen.Select(
            Gen.Int[1, 10],
            Gen.Int[0, 1_000_000]);

        gen.Sample(t =>
        {
            var (retryLimit, seed) = t;

            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new FakeTimeProvider(start);
            var store = new InMemoryOutboxStore(clock);
            var client = new AlwaysUnreachableClient();

            var options = new LogsOutboxOptions
            {
                RetryLimit = retryLimit,
                PollInterval = TimeSpan.FromSeconds(5),
                BatchSize = 100,
            };

            // Seed a single pending entry.
            var payload = new LogPayload
            {
                ContainerKey = "c",
                Source = "s",
                Owner = "o",
                Action = "a",
                UserId = "u",
                Content = $"entry-{seed}",
            };
            var write = store.WriteAsync(payload).GetAwaiter().GetResult();
            Assert.Equal(WriteOutcome.Persisted, write.Outcome);

            var entryId = ReadPendingNow(store)
                .Single(e => e.Payload.Content == payload.Content)
                .Id;

            var worker = new DispatchWorker(store, client, options, clock);

            // Amount to advance between cycles: past the poll interval AND past the longest possible
            // backoff (the exponential policy caps at 300s) so the entry's NextAttemptAt is always
            // elapsed and the worker's Task.Delay completes before the next cycle.
            var advance = options.PollInterval + RetryBackoffPolicy.MaxDelay + TimeSpan.FromSeconds(1);

            // Bound the number of cycles well above RetryLimit so a stuck worker fails rather than
            // hangs. One send happens per eligible cycle; the entry becomes Failed on the send whose
            // resulting attempt count reaches RetryLimit.
            var maxCycles = retryLimit + 5;

            worker.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
            try
            {
                for (var cycle = 0; cycle < maxCycles; cycle++)
                {
                    // The cycle at index `cycle` performs send number `cycle + 1`, capped at
                    // RetryLimit (no further sends occur once the entry is Failed).
                    var expectedSends = Math.Min(cycle + 1, retryLimit);

                    // Wait for the worker to complete this cycle's delivery attempt.
                    WaitUntil(() => client.SendCount >= expectedSends);

                    // Invariant (a): the running attempt count never exceeds the retry limit.
                    Assert.True(client.SendCount <= retryLimit,
                        $"send count {client.SendCount} exceeded retry limit {retryLimit}");

                    // Once we have driven RetryLimit sends the entry must be terminal; stop cycling.
                    if (client.SendCount >= retryLimit)
                    {
                        break;
                    }

                    // Advance the clock so the backoff elapses and the poll delay completes, making
                    // the entry eligible for the next cycle.
                    clock.Advance(advance);
                }
            }
            finally
            {
                // Wait for the terminal transition to settle: a Failed entry disappears from the
                // pending read even with the widest bounds. This avoids observing the brief window
                // between the final send and the MarkFailed write.
                WaitUntil(() => !ReadPendingNow(store).Any(e => e.Id == entryId));
                worker.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            }

            // Invariant (a): total attempts never exceeded the retry limit, and reached it exactly.
            Assert.True(client.SendCount <= retryLimit,
                $"final send count {client.SendCount} exceeded retry limit {retryLimit}");
            Assert.Equal(retryLimit, client.SendCount);

            // Invariant (b): once attempts reach the limit the entry is marked Failed. A Failed entry
            // is excluded from the pending query, so advancing the clock arbitrarily far and reading
            // with the widest possible bounds must never return it.
            clock.Advance(TimeSpan.FromDays(1));

            // Invariant (c): a Failed entry is excluded from all subsequent pending batches.
            var laterBatch = ReadPendingNow(store);
            Assert.DoesNotContain(laterBatch, e => e.Id == entryId);
        }, iter: 100);
    }

    /// <summary>
    /// Reads all currently-eligible pending entries with the widest possible bounds so that any
    /// entry that is still Pending with an elapsed backoff is visible.
    /// </summary>
    private static IReadOnlyList<OutboxEntry> ReadPendingNow(InMemoryOutboxStore store) =>
        store.GetPendingAsync(int.MaxValue, int.MaxValue).GetAwaiter().GetResult();

    /// <summary>
    /// Spin-waits (bounded) until the predicate holds, yielding so the worker's background tasks can
    /// run. The bound keeps a wedged worker from hanging the test run.
    /// </summary>
    private static void WaitUntil(Func<bool> predicate)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!predicate() && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(1);
        }
    }
}
