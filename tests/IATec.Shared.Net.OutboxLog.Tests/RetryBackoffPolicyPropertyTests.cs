using CsCheck;
using IATec.Shared.Net.OutboxLog.Dispatch;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for <see cref="RetryBackoffPolicy"/>.
/// </summary>
public class RetryBackoffPolicyPropertyTests
{
    private static readonly TimeSpan Cap = TimeSpan.FromSeconds(300);

    // Feature: logs-outbox-library, Property 14: Exponential backoff is monotonic and capped at 300 seconds
    // Validates: Requirements 5.7, 8.3
    [Fact]
    public void ExponentialBackoffIsMonotonicAndCappedAt300Seconds()
    {
        // Attempt counts are one-based (>= 1). Cover well past the cap (2^9 = 512s >= 300s
        // clamps from attempt 10 onward) and up to large values to exercise the overflow guard.
        Gen.Int[1, 100_000]
            .Sample(attempt =>
            {
                TimeSpan delay = RetryBackoffPolicy.ComputeDelay(attempt);

                // Below the cap the delay equals 2^(attempt-1) seconds; otherwise it clamps to 300s.
                double uncapped = Math.Pow(2, attempt - 1);
                TimeSpan expected = uncapped >= Cap.TotalSeconds
                    ? Cap
                    : TimeSpan.FromSeconds(uncapped);
                Assert.Equal(expected, delay);

                // The delay never exceeds the 300-second cap.
                Assert.True(delay <= Cap, $"delay {delay} exceeded cap {Cap} for attempt {attempt}");

                // Monotonic (non-decreasing): ComputeDelay(n) <= ComputeDelay(n+1).
                TimeSpan next = RetryBackoffPolicy.ComputeDelay(attempt + 1);
                Assert.True(delay <= next, $"backoff decreased: ComputeDelay({attempt})={delay} > ComputeDelay({attempt + 1})={next}");
            }, iter: 100);
    }
}
