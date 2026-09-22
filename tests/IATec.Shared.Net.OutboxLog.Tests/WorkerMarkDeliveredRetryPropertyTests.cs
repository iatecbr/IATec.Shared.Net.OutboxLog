using System.Collections.Concurrent;
using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for the <see cref="DispatchWorker"/> mark-delivered retry path: when
/// persisting the delivered status repeatedly fails, the worker must bound its marking attempts,
/// leave the entry in its pre-marking state, and surface an error.
/// </summary>
public class WorkerMarkDeliveredRetryPropertyTests
{
    /// <summary>Matches <c>DispatchWorker.MarkDeliveredMaxAttempts</c>.</summary>
    private const int MarkDeliveredMaxAttempts = 3;

    /// <summary>
    /// A store decorator whose <see cref="MarkDeliveredAsync"/> always throws (simulating a
    /// persistence fault) while counting invocations per entry. All other operations delegate to a
    /// real <see cref="InMemoryOutboxStore"/>, so the entry's actual persisted state is whatever the
    /// inner store holds. Because marking never succeeds, that state stays Pending.
    /// </summary>
    private sealed class FaultingMarkStore : IOutboxStore
    {
        private readonly InMemoryOutboxStore _inner;
        private readonly ConcurrentDictionary<Guid, int> _markAttempts = new();

        public FaultingMarkStore(InMemoryOutboxStore inner) => _inner = inner;

        public bool IsAvailable => _inner.IsAvailable;

        /// <summary>Total number of times <see cref="MarkDeliveredAsync"/> was called for the entry.</summary>
        public int MarkAttemptsFor(Guid entryId) => _markAttempts.TryGetValue(entryId, out var n) ? n : 0;

        public Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
            => _inner.WriteAsync(payload, ct);

        public Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
            => _inner.GetPendingAsync(batchSize, retryLimit, ct);

        public Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _markAttempts.AddOrUpdate(entryId, 1, static (_, n) => n + 1);
            throw new InvalidOperationException("Simulated persistence fault marking entry delivered.");
        }

        public Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default)
            => _inner.IncrementAttemptAsync(entryId, nextAttemptAt, ct);

        public Task MarkFailedAsync(Guid entryId, CancellationToken ct = default)
            => _inner.MarkFailedAsync(entryId, ct);
    }

    /// <summary>An always-accepting client so the worker proceeds to the marking step every time.</summary>
    private sealed class AcceptingClient : ILogBankClient
    {
        public Task<DeliveryResult> SendAsync(LogPayload payload, CancellationToken ct)
            => Task.FromResult(new DeliveryResult(DeliveryOutcome.Accepted, 200));
    }

    /// <summary>Captures whether at least one error-level entry was logged, for surfacing checks.</summary>
    private sealed class ErrorCapturingLogger : ILogger<DispatchWorker>
    {
        private volatile bool _errorLogged;

        public bool ErrorLogged => _errorLogged;

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
            {
                _errorLogged = true;
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }

    // Feature: logs-outbox-library, Property 15: Mark-delivered failures are bounded and non-destructive
    // Validates: Requirements 6.5
    [Fact]
    public void MarkDeliveredFailures_AreBoundedAndNonDestructive()
    {
        // Generate varied, distinct payloads so each iteration exercises a different entry.
        var genPayload = Gen.Select(
            Gen.String[0, 32],
            Gen.String[0, 32],
            Gen.String[0, 32],
            Gen.String[0, 32],
            Gen.Int[0, 1_000_000],
            (owner, action, userId, content, seed) => new LogPayload
            {
                ContainerKey = "c",
                Source = "s",
                Owner = owner,
                Action = action,
                UserId = userId,
                // The seed guarantees a unique deduplication key even when the random text collides.
                Content = $"{content}-{seed}",
            });

        genPayload.Sample(payload =>
        {
            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var clock = new FakeTimeProvider(start);

            var innerStore = new InMemoryOutboxStore(clock);
            var store = new FaultingMarkStore(innerStore);
            var client = new AcceptingClient();
            var logger = new ErrorCapturingLogger();
            var options = new LogsOutboxOptions
            {
                PollInterval = TimeSpan.FromSeconds(5),
                BatchSize = 100,
                RetryLimit = 5,
            };

            // Seed a single pending entry the worker will pick up on its first cycle.
            var write = store.WriteAsync(payload).GetAwaiter().GetResult();
            Assert.Equal(WriteOutcome.Persisted, write.Outcome);

            var entry = store
                .GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult()
                .Single();
            var entryId = entry.Id;

            var worker = new DispatchWorker(store, client, options, clock, logger);

            // Run exactly one deterministic poll cycle. Inside it the worker fetches the entry,
            // the client accepts, and the bounded mark-delivered retries all run synchronously.
            worker.RunSingleCycleForTestsAsync().GetAwaiter().GetResult();

            // (a) Marking is bounded: at most MarkDeliveredMaxAttempts calls for the entry in one delivery.
            var attempts = store.MarkAttemptsFor(entryId);
            Assert.True(
                attempts is > 0 and <= MarkDeliveredMaxAttempts,
                $"expected 1..{MarkDeliveredMaxAttempts} marking attempts but observed {attempts}");

            // (b) Non-destructive: the entry stays in its pre-marking (Pending) state because the
            // delivered state was never persisted. It remains eligible for a later delivery cycle.
            var after = store
                .GetPendingAsync(int.MaxValue, int.MaxValue)
                .GetAwaiter().GetResult()
                .SingleOrDefault(e => e.Id == entryId);
            Assert.NotNull(after);
            Assert.Equal(DeliveryStatus.Pending, after!.Status);
            Assert.Null(after.DeliveredAt);

            // (c) An error is surfaced when the status could not be persisted after the retries.
            Assert.True(logger.ErrorLogged, "expected an error to be surfaced after persistent marking failure");
        }, iter: 100);
    }
}
