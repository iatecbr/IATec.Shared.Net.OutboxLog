using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for the <see cref="DispatchWorker"/> shutdown path: when the worker is
/// cancelled mid-cycle, every entry that was not delivered must remain
/// <see cref="DeliveryStatus.Pending"/> with its data unchanged, so it is available for delivery
/// on the next startup.
/// </summary>
public class WorkerShutdownPreservationPropertyTests
{
    /// <summary>
    /// A fake <see cref="ILogBankClient"/> that never completes a delivery on its own: every
    /// <see cref="SendAsync"/> call blocks until the shutdown token is cancelled and then
    /// surfaces the cancellation. This guarantees that once shutdown is requested no entry is
    /// ever delivered, so the store's final state reflects only what shutdown preserved.
    /// </summary>
    private sealed class NeverCompletingClient : ILogBankClient
    {
        public async Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
        {
            // Block indefinitely; only cancellation (shutdown) releases the wait, which the worker
            // treats as "in-flight delivery not completed" and leaves the entry Pending.
            await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
            return new DeliveryResult(DeliveryOutcome.Accepted, 200);
        }
    }

    // Feature: logs-outbox-library, Property 17: Shutdown preserves un-dispatched entries
    // Validates: Requirements 8.6
    [Fact]
    public void Shutdown_PreservesUnDispatchedEntries()
    {
        // Generate a batch of 1..8 distinct payloads (distinct content => distinct dedup keys),
        // exercising the property across many generated seeds and batch shapes.
        var gen = Gen.Int[0, 1_000_000].Array[1, 8];

        gen.Sample(seeds =>
        {
            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new FakeTimeProvider(start);
            var store = new InMemoryOutboxStore(clock);

            var options = new LogsOutboxOptions
            {
                PollInterval = TimeSpan.FromSeconds(5),
                BatchSize = 100,
                RetryLimit = 3,
            };

            // Seed one pending entry per generated seed. A distinct index guarantees a unique
            // deduplication key per write so every entry is persisted (not deduplicated).
            for (var i = 0; i < seeds.Length; i++)
            {
                clock.Advance(TimeSpan.FromMilliseconds(1));
                var payload = new LogPayload
                {
                    ContainerKey = "c",
                    Source = "s",
                    Owner = "o",
                    Action = "a",
                    UserId = "u",
                    Content = $"entry-{i}-{seeds[i]}",
                };

                var write = store.WriteAsync(payload).GetAwaiter().GetResult();
                Assert.Equal(WriteOutcome.Persisted, write.Outcome);
            }

            var expectedCount = seeds.Length;

            // Capture a snapshot of the seeded entries (id -> status/attempt/payload) so we can
            // assert nothing changed after shutdown.
            var before = store.GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult();
            Assert.Equal(expectedCount, before.Count);

            var snapshot = before.ToDictionary(
                e => e.Id,
                e => (e.Status, e.AttemptCount, e.Payload.Content, e.DeduplicationKey));

            var worker = new DispatchWorker(store, new NeverCompletingClient(), options, clock);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            // The worker runs its first poll cycle immediately on start. Because the client never
            // completes a delivery, every entry is "in flight" and none is delivered before we
            // request shutdown.
            worker.StartAsync(cts.Token).GetAwaiter().GetResult();

            try
            {
                // Let the worker begin its first cycle and enter the (blocking) delivery, then
                // request shutdown. StopAsync cancels the stopping token; the drain timer is
                // driven by the fake clock, so advancing past the 30s bound guarantees the
                // in-flight delivery is cancelled deterministically rather than hanging.
                Thread.Sleep(20);
                var stopTask = worker.StopAsync(CancellationToken.None);

                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (!stopTask.IsCompleted && sw.Elapsed < TimeSpan.FromSeconds(5))
                {
                    // Advance past the shutdown drain bound so the drain timer fires and cancels
                    // the blocked delivery, letting shutdown complete.
                    clock.Advance(TimeSpan.FromSeconds(31));
                    Thread.Sleep(10);
                }

                stopTask.GetAwaiter().GetResult();
            }
            finally
            {
                worker.Dispose();
            }

            // After shutdown every un-dispatched entry must remain Pending and unchanged. Pending
            // entries with attempt count below the retry limit are exactly what GetPendingAsync
            // returns, so the surviving set must equal the seeded set with identical data.
            var after = store.GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult();

            Assert.Equal(expectedCount, after.Count);

            foreach (var entry in after)
            {
                Assert.True(snapshot.TryGetValue(entry.Id, out var original),
                    "Shutdown must not introduce or lose entries.");

                // Status still Pending, attempt count untouched (never incremented), payload and
                // dedup key identical: the entry is exactly as it was before shutdown.
                Assert.Equal(DeliveryStatus.Pending, entry.Status);
                Assert.Equal(original.Status, entry.Status);
                Assert.Equal(0, entry.AttemptCount);
                Assert.Equal(original.AttemptCount, entry.AttemptCount);
                Assert.Equal(original.Content, entry.Payload.Content);
                Assert.Equal(original.DeduplicationKey, entry.DeduplicationKey);
                Assert.Null(entry.DeliveredAt);
            }
        }, iter: 100);
    }
}
