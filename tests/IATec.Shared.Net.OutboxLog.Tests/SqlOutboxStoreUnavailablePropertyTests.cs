using CsCheck;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based test for the availability gate of <see cref="SqlOutboxStore{TDbContext}"/>.
/// When the backing table is unavailable (<see cref="SqlOutboxTableInitializer{TDbContext}.TableAvailable"/>
/// is <c>false</c>), every read and write operation must short-circuit and reject/no-op WITHOUT
/// touching the database (Req 4.4). We prove "no DB command issued" with a spy
/// <see cref="IDbContextFactory{TContext}"/> that throws on any create call: an unavailable store
/// must never obtain a context, so the spy's create-count stays at zero.
/// </summary>
public class SqlOutboxStoreUnavailablePropertyTests
{
    /// <summary>
    /// A DbContext wired to a SQL Server provider with a dummy connection string. It is never
    /// expected to open a connection: any accidental DB access would require a live server and fail
    /// loudly, which is exactly the behaviour we want to guard against.
    /// </summary>
    private sealed class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options)
            : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
    }

    /// <summary>
    /// A spy factory that fails loudly if asked to create a context. Because an unavailable store
    /// must not touch the database, invoking this factory is itself a property violation. It also
    /// records a create-count so the test can assert it stayed at zero.
    /// </summary>
    private sealed class ThrowingDbContextFactory : IDbContextFactory<TestDbContext>
    {
        public int CreateCount { get; private set; }

        public TestDbContext CreateDbContext()
        {
            CreateCount++;
            throw new InvalidOperationException(
                "Unavailable store must not create a DbContext (no DB command allowed).");
        }

        public Task<TestDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
        {
            CreateCount++;
            throw new InvalidOperationException(
                "Unavailable store must not create a DbContext asynchronously (no DB command allowed).");
        }
    }

    /// <summary>
    /// Builds a consumer context on a lightweight relational provider (Sqlite). Its
    /// change tracker is never expected to be touched by an unavailable store.
    /// </summary>
    private static TestDbContext CreateConsumerContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite("DataSource=:memory:")
            .Options;

        return new TestDbContext(options);
    }

    private static readonly Gen<string> GenField =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const(":"),
            Gen.Const("\u0000"),
            Gen.Const("héllo-\u00e9\u4e2d\u6587"),
            Gen.String);

    private static readonly Gen<LogPayload> GenPayload =
        Gen.Select(GenField, GenField, GenField, GenField, GenField, GenField,
            (containerKey, source, owner, action, userId, content) => new LogPayload
            {
                ContainerKey = containerKey,
                Source = source,
                Owner = owner,
                Action = action,
                UserId = userId,
                Content = content,
            });

    // Feature: logs-outbox-library, Property 10: An unavailable store rejects all operations without touching the database
    // Validates: Requirements 4.4
    [Fact]
    public void UnavailableStore_RejectsAllOperations_WithoutTouchingDatabase()
    {
        // Two arbitrary Guids per iteration: one to target status-update operations, one extra so
        // we cover distinct ids. Combined with an arbitrary payload for the write path.
        Gen.Select(GenPayload, Gen.Guid, Gen.Guid, Gen.DateTimeOffset)
            .Sample(t =>
            {
                var (payload, entryId, otherId, nextAttemptAt) = t;

                var factory = new ThrowingDbContextFactory();

                // A freshly-constructed initializer has never run InitializeAsync, so TableAvailable
                // is false — the "unavailable" state we are exercising.
                var initializer = new SqlOutboxTableInitializer<TestDbContext>(
                    factory,
                    tableName: null,
                    logger: NullLogger<SqlOutboxTableInitializer<TestDbContext>>.Instance);

                Assert.False(initializer.TableAvailable);

                using var consumerContext = CreateConsumerContext();

                var store = new SqlOutboxStore<TestDbContext>(
                    consumerContext,
                    factory,
                    initializer,
                    timeProvider: null,
                    logger: null);

                // The store must report itself unavailable.
                Assert.False(store.IsAvailable);

                // WriteAsync => Failed with a non-null error and Success == false.
                var write = store.WriteAsync(payload).GetAwaiter().GetResult();
                Assert.Equal(WriteOutcome.Failed, write.Outcome);
                Assert.False(write.Success);
                Assert.NotNull(write.Error);

                // GetPendingAsync => empty batch.
                var pending = store
                    .GetPendingAsync(batchSize: 100, retryLimit: 5)
                    .GetAwaiter().GetResult();
                Assert.Empty(pending);

                // Status updates complete without throwing (they no-op while unavailable).
                store.MarkDeliveredAsync(entryId).GetAwaiter().GetResult();
                store.IncrementAttemptAsync(otherId, nextAttemptAt).GetAwaiter().GetResult();
                store.MarkFailedAsync(entryId).GetAwaiter().GetResult();

                // No operation may have obtained a context: no DB command was issued.
                Assert.Equal(0, factory.CreateCount);
            }, iter: 100);
    }
}
