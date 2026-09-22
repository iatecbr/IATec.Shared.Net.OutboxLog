using System.Diagnostics;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Shared synchronization helpers for <see cref="DispatchWorker"/> tests.
/// </summary>
/// <remarks>
/// <para>
/// The dispatch worker is a real <see cref="Microsoft.Extensions.Hosting.BackgroundService"/>: its
/// poll loop runs on the thread pool and its cadence is driven by a
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>. That means a cycle's observable
/// effect (a send, a delivered entry, an incremented attempt, a poll count) appears asynchronously
/// shortly after <c>StartAsync</c> returns or shortly after the fake clock is advanced — never
/// synchronously in lockstep with the calling thread.
/// </para>
/// <para>
/// These helpers replace fragile "the cycle already ran / has not run yet" assumptions with active
/// waits that poll an observable condition against a generous real-time deadline. The deadlines are
/// wide (multiple seconds) so the tests stay stable on loaded CI machines, and every wait is bounded
/// so a genuinely wedged worker surfaces as a failure rather than a hang.
/// </para>
/// </remarks>
internal static class WorkerTestSync
{
    /// <summary>
    /// Generous real-time deadline for a single cycle's effect to become observable. Sized for a
    /// heavily loaded CI box where the thread pool may be momentarily starved, not for the happy
    /// path (where the effect appears in well under a millisecond).
    /// </summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Spin-waits until <paramref name="condition"/> holds or <paramref name="timeout"/> elapses,
    /// yielding the thread between checks so the worker's background loop can make progress.
    /// Returns <see langword="true"/> when the condition was met.
    /// </summary>
    public static bool WaitUntil(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = Stopwatch.StartNew();
        var limit = timeout ?? DefaultTimeout;

        while (deadline.Elapsed < limit)
        {
            if (condition())
            {
                return true;
            }

            Thread.Sleep(2);
        }

        return condition();
    }

    /// <summary>
    /// Actively waits until <paramref name="condition"/> holds, throwing a descriptive
    /// <see cref="TimeoutException"/> when the deadline elapses first so a stuck worker fails the
    /// test instead of hanging it.
    /// </summary>
    public static void WaitUntilOrThrow(Func<bool> condition, string description, TimeSpan? timeout = null)
    {
        var limit = timeout ?? DefaultTimeout;
        if (!WaitUntil(condition, limit))
        {
            throw new TimeoutException($"Condition not met within {limit}: {description}");
        }
    }
}
