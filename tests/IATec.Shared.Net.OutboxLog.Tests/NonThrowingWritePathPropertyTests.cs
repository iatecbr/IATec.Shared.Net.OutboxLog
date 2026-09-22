using CsCheck;
using IATec.Shared.Net.OutboxLog.Configuration;
using IATec.Shared.Net.OutboxLog.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests covering the capture-layer guarantee that the write path never throws to
/// the calling application, even when the underlying store faults. Both a synchronously-throwing
/// <see cref="IOutboxStore.WriteAsync"/> and a faulted-Task-returning <see cref="IOutboxStore.WriteAsync"/>
/// are exercised.
/// </summary>
public class NonThrowingWritePathPropertyTests
{
    private static readonly LogLevel[] Levels =
    [
        LogLevel.Trace,
        LogLevel.Debug,
        LogLevel.Information,
        LogLevel.Warning,
        LogLevel.Error,
        LogLevel.Critical,
    ];

    private static readonly Gen<LogLevel> GenLevel = Gen.Int[0, Levels.Length - 1].Select(i => Levels[i]);

    private static readonly Gen<EventId> GenEventId =
        Gen.Select(
            Gen.Int[0, 10_000],
            Gen.OneOf(Gen.Const((string?)null), Gen.Const(""), Gen.Const("action-name"), Gen.String),
            (id, name) => new EventId(id, name));

    private static readonly Gen<string> GenMessage =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const("héllo-\u00e9\u4e2d\u6587"),
            Gen.String);

    private static readonly Gen<Exception?> GenException =
        Gen.OneOf(
            Gen.Const((Exception?)null),
            Gen.Const((Exception?)new InvalidOperationException("boom")),
            Gen.Const((Exception?)new ApplicationException("state error")));

    /// <summary>How the faulting store surfaces its failure.</summary>
    private enum FaultMode
    {
        /// <summary>WriteAsync throws synchronously before returning a task.</summary>
        SynchronousThrow,

        /// <summary>WriteAsync returns an already-faulted task.</summary>
        FaultedTask,
    }

    private static readonly Gen<FaultMode> GenFaultMode =
        Gen.OneOf(Gen.Const(FaultMode.SynchronousThrow), Gen.Const(FaultMode.FaultedTask));

    /// <summary>
    /// A store whose <see cref="WriteAsync"/> always fails, either by throwing synchronously or by
    /// returning a faulted task. Read/update operations are inert.
    /// </summary>
    private sealed class FaultingOutboxStore(FaultMode mode) : IOutboxStore
    {
        public bool IsAvailable => true;

        public Task<WriteResult> WriteAsync(LogPayload payload, CancellationToken ct = default)
        {
            if (mode == FaultMode.SynchronousThrow)
            {
                throw new InvalidOperationException("Injected synchronous write failure");
            }

            return Task.FromException<WriteResult>(new InvalidOperationException("Injected faulted-task write failure"));
        }

        public Task<IReadOnlyList<OutboxEntry>> GetPendingAsync(int batchSize, int retryLimit, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<OutboxEntry>>([]);

        public Task MarkDeliveredAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;

        public Task IncrementAttemptAsync(Guid entryId, DateTimeOffset nextAttemptAt, CancellationToken ct = default) => Task.CompletedTask;

        public Task MarkFailedAsync(Guid entryId, CancellationToken ct = default) => Task.CompletedTask;
    }

    private static Microsoft.Extensions.Logging.ILogger CreateLogger(FaultMode mode)
    {
        var provider = new OutboxLoggerProvider(
            new FaultingOutboxStore(mode),
            new LogPayloadFactory(),
            new LogsOutboxOptions());

        return provider.CreateLogger("Test.Category");
    }

    // Feature: logs-outbox-library, Property 4: The write path never throws to the caller
    // Validates: Requirements 2.5, 8.2
    [Fact]
    public void WritePath_NeverThrows_WhenStoreFaults()
    {
        Gen.Select(GenFaultMode, GenLevel, GenEventId, GenMessage, GenException)
            .Sample(t =>
            {
                var (mode, level, eventId, message, exception) = t;
                var logger = CreateLogger(mode);

                // Calling Log directly with a faulting store must complete normally.
                logger.Log(
                    level,
                    eventId,
                    message,
                    exception,
                    static (state, ex) => ex is null ? state : state + " | " + ex.Message);

                // The high-level extension helpers route through Log and must also not throw.
                logger.LogInformation(eventId, exception, "{Message}", message);
            }, iter: 100);
    }
}
