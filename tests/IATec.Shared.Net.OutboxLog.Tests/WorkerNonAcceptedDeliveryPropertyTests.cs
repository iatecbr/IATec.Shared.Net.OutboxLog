using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for the <see cref="DispatchWorker"/> behaviour on a non-accepting
/// delivery outcome.
/// </summary>
public class WorkerNonAcceptedDeliveryPropertyTests
{
    /// <summary>
    /// A stub <see cref="ILogBankClient"/> that returns a fixed non-accepting
    /// <see cref="DeliveryResult"/> for every send, counting the number of sends.
    /// </summary>
    private sealed class NonAcceptingClient : ILogBankClient
    {
        private readonly DeliveryResult _result;
        private int _sendCount;

        public NonAcceptingClient(DeliveryResult result) => _result = result;

        public int SendCount => Volatile.Read(ref _sendCount);

        public Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
        {
            Interlocked.Increment(ref _sendCount);
            return Task.FromResult(_result);
        }
    }

    /// <summary>The three outcomes that must not be treated as an acceptance by the worker.</summary>
    private static readonly Gen<DeliveryOutcome> GenNonAcceptingOutcome =
        Gen.OneOfConst(DeliveryOutcome.Rejected, DeliveryOutcome.TimedOut, DeliveryOutcome.Unreachable);

    /// <summary>
    /// Status codes that may accompany a non-accepting outcome: a non-2xx code for a response,
    /// or <c>null</c> when no response was received (timeout / unreachable).
    /// </summary>
    private static readonly Gen<int?> GenStatusCode =
        Gen.Frequency(
            (1, Gen.Int[0, 0].Select(_ => (int?)null)),
            (3, Gen.Int[400, 599].Select(c => (int?)c)));

    // Feature: logs-outbox-library, Property 12: A non-accepted delivery increments the attempt and preserves the worker
    // Validates: Requirements 5.4, 8.1
    [Fact]
    public void NonAcceptedDelivery_IncrementsAttemptOnce_KeepsPending_AndWorkerSurvives()
    {
        var gen = Gen.Select(
            GenNonAcceptingOutcome,
            GenStatusCode,
            // A varied payload so the deduplication key differs across iterations.
            Gen.Int[0, 1_000_000]);

        gen.Sample(t =>
        {
            var (outcome, statusCode, seed) = t;

            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new FakeTimeProvider(start);
            var store = new InMemoryOutboxStore(clock);

            // RetryLimit high enough that a single non-accept keeps the entry below the limit,
            // so the worker increments (Pending) rather than marking Failed.
            var options = new LogsOutboxOptions
            {
                RetryLimit = 10,
                PollInterval = TimeSpan.FromSeconds(5),
                BatchSize = 100,
            };

            var client = new NonAcceptingClient(new DeliveryResult(outcome, statusCode));

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

            var seeded = store
                .GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult()
                .Single(e => e.Payload.Content == payload.Content);
            Assert.Equal(0, seeded.AttemptCount);
            Assert.Equal(DeliveryStatus.Pending, seeded.Status);

            var worker = new DispatchWorker(store, client, options, clock);

            // Run exactly one deterministic poll cycle: the entry is delivered once and, being
            // non-accepted, its attempt is incremented and a future NextAttemptAt (backoff) is set.
            worker.RunSingleCycleForTestsAsync().GetAwaiter().GetResult();

            // Exactly one send happened for the single entry.
            Assert.Equal(1, client.SendCount);

            // The increment set a future NextAttemptAt, so the entry is excluded from an immediate
            // (un-advanced) pending read. Advance past the maximum possible backoff and re-read it.
            clock.Advance(RetryBackoffPolicy.MaxDelay + TimeSpan.FromSeconds(1));

            var observed = store
                .GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult()
                .SingleOrDefault(e => e.Id == seeded.Id);

            Assert.NotNull(observed);

            // The attempt count increased by exactly one, and the entry remains Pending.
            Assert.Equal(1, observed!.AttemptCount);
            Assert.Equal(DeliveryStatus.Pending, observed.Status);

            // The worker survives a non-accepting delivery (Req 8.1): running another cycle does not
            // throw and does not terminate the worker's cycle logic.
            var secondCycle = Record.Exception(
                () => worker.RunSingleCycleForTestsAsync().GetAwaiter().GetResult());
            Assert.Null(secondCycle);
        }, iter: 100);
    }
}