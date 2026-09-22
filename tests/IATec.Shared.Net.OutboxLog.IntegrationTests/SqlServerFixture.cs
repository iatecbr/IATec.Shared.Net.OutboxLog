// Feature: logs-outbox-library, Property 16: Transactional atomicity of log production
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.IntegrationTests;

/// <summary>
/// Shared xUnit collection so a single SQL Server container is spun up for all tests that need it.
/// </summary>
[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "SqlServer";
}

/// <summary>
/// Boots a real SQL Server instance via Testcontainers and creates the outbox table once.
/// If Docker is unavailable (or the container fails to start), the fixture records the reason
/// and reports <see cref="IsAvailable"/> as <c>false</c> so tests skip cleanly rather than fail.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    /// <summary><c>true</c> when a SQL Server container started and the table was created.</summary>
    public bool IsAvailable { get; private set; }

    /// <summary>Human-readable reason the fixture is unavailable, used as the skip message.</summary>
    public string? SkipReason { get; private set; }

    /// <summary>Connection string to the running container, or <c>null</c> when unavailable.</summary>
    public string? ConnectionString { get; private set; }

    public async Task InitializeAsync()
    {
        try
        {
            _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
            await _container.StartAsync();
            ConnectionString = _container.GetConnectionString();

            // Create the outbox table once for the container using the same initializer the
            // library uses in production (idempotent).
            var factory = new TestDbContextFactory(ConnectionString);
            var initializer = new SqlOutboxTableInitializer<TestDbContext>(
                factory,
                OutboxEntryConfiguration.DefaultTableName,
                NullLogger<SqlOutboxTableInitializer<TestDbContext>>.Instance);
            await initializer.InitializeAsync(CancellationToken.None);

            IsAvailable = initializer.TableAvailable;
            if (!IsAvailable)
            {
                SkipReason = "Outbox table could not be created on the SQL Server container.";
            }
        }
        catch (Exception ex)
        {
            // Most commonly: Docker is not installed/running in this environment.
            IsAvailable = false;
            SkipReason = $"SQL Server test container is unavailable: {ex.Message}";

            if (_container is not null)
            {
                try
                {
                    await _container.DisposeAsync();
                }
                catch
                {
                    // Best effort cleanup; ignore secondary failures.
                }

                _container = null;
            }
        }
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }
}
