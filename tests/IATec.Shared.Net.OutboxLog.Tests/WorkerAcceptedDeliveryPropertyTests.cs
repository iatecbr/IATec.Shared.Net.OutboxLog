using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for the <see cref="DispatchWorker"/> delivery path: when the log bank
/// client accepts a payload, the corresponding outbox entry must transition to
/// <see cref="DeliveryStatus.Delivered"/>.
/// </summary>
public class WorkerAcceptedDeliveryPropertyTests
{
    /// <summary>
    /// A fake <see cref="ILogBankClient"/> that always accepts. This isolates the property under
    /// test (accepted delivery => entry delivered) from any real network behavior.
    /// </summary>
    private sealed class AlwaysAcceptingClient : ILogBankClient
    {
        public Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
            => Task.FromResult(new DeliveryResult(DeliveryOutcome.Accepted, 200));
    }

    // Feature: logs-outbox-library, Property 11: An accepted delivery marks the entry delivered
    // Validates: Requirements 5.3
    [Fact]
    public void AcceptedDelivery_MarksEntryDelivered()
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

            // Sanity: all seeded entries start pending.
            var pendingAtStart = store.GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult();
            Assert.Equal(expectedCount, pendingAtStart.Count);

            var worker = new DispatchWorker(store, new AlwaysAcceptingClient(), options, clock);

            // Run exactly one poll cycle deterministically (no BackgroundService, no scheduler race).
            // A single cycle fetches the whole pending batch and delivers each entry.
            worker.RunSingleCycleForTestsAsync().GetAwaiter().GetResult();

            // Delivered entries are excluded from GetPendingAsync, so an empty pending set is the
            // observable "all delivered" signal.
            var remaining = store.GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult().Count;

            Assert.Equal(0, remaining);
        }, iter: 100);
    }
}