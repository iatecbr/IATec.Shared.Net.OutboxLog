namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Computes the exponential backoff delay applied by the dispatch worker between
/// delivery retries of a failed outbox entry.
/// </summary>
public static class RetryBackoffPolicy
{
    /// <summary>The maximum backoff delay. The computed delay is never longer than this.</summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(300);

    /// <summary>
    /// Exponential backoff for the resilience path: 1s, doubling per failure, capped at 300s.
    /// <para><c>backoff(attempt) = min(2^(attempt-1) seconds, 300s)</c>.</para>
    /// </summary>
    /// <param name="attemptCount">
    /// The one-based attempt number (the first attempt is <c>1</c>). Values less than
    /// <c>1</c> are treated as <c>1</c>.
    /// </param>
    /// <returns>
    /// The delay to wait before the next attempt: <c>2^(attemptCount-1)</c> seconds while
    /// below the cap, otherwise <see cref="MaxDelay"/> (300 seconds). The result is
    /// non-decreasing as <paramref name="attemptCount"/> increases and never exceeds the cap.
    /// </returns>
    public static TimeSpan ComputeDelay(int attemptCount)
    {
        // Normalize: the policy is defined for attempt counts >= 1.
        if (attemptCount < 1)
        {
            attemptCount = 1;
        }

        int exponent = attemptCount - 1;

        // 2^9 = 512 >= 300, so any exponent >= 9 clamps to the cap. Short-circuiting here
        // also guards against integer/double overflow for very large attempt counts.
        if (exponent >= 9)
        {
            return MaxDelay;
        }

        int seconds = 1 << exponent; // 2^exponent for exponent in [0, 8] -> 1..256
        return seconds >= MaxDelay.TotalSeconds
            ? MaxDelay
            : TimeSpan.FromSeconds(seconds);
    }
}
