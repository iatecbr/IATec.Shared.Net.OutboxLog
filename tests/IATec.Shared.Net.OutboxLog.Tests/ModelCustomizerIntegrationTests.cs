using IATec.Shared.Net.OutboxLog;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Verifica la Opcion 2: el outbox se integra en un DbContext de negocio SIN que este llame a
/// ApplyConfiguration(new OutboxEntryConfiguration()) en su OnModelCreating. La entidad se inyecta
/// de forma transparente via OutboxModelCustomizer (activado con AddLogsOutboxModel()), y la
/// atomicidad transaccional se conserva porque el log vive en el MISMO context de negocio.
/// </summary>
public class ModelCustomizerIntegrationTests
{
    public sealed class BusinessRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // DbContext de negocio: NOTA que su OnModelCreating NO menciona el outbox para nada.
    public sealed class BusinessOnlyDbContext : DbContext
    {
        public BusinessOnlyDbContext(DbContextOptions<BusinessOnlyDbContext> options) : base(options) { }

        public DbSet<BusinessRow> BusinessRows => Set<BusinessRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<BusinessRow>().ToTable("BusinessRows");
            // <- Intencionalmente NO se aplica OutboxEntryConfiguration aqui.
        }
    }

    // Factory que activa el modelo del outbox via AddLogsOutboxModel() sobre la MISMA conexion.
    private sealed class Factory : IDbContextFactory<BusinessOnlyDbContext>
    {
        private readonly SqliteConnection _connection;
        public Factory(SqliteConnection connection) => _connection = connection;

        public BusinessOnlyDbContext CreateDbContext()
        {
            var options = new DbContextOptionsBuilder<BusinessOnlyDbContext>()
                .UseSqlite(_connection)
                .AddLogsOutboxModel()   // <- inyecta OutboxEntry sin tocar OnModelCreating
                .Options;
            return new BusinessOnlyDbContext(options);
        }
    }

    [Fact]
    public async Task OutboxEntity_IsInjected_WithoutApplyConfiguration_AndInitializerCreatesTable()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();

        var factory = new Factory(connection);

        // Crear solo la tabla de negocio (simula migraciones previas de la app).
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"BusinessRows\" (\"Id\" INTEGER NOT NULL CONSTRAINT \"PK_BusinessRows\" PRIMARY KEY AUTOINCREMENT, \"Name\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }

        // El modelo debe conocer OutboxEntry aunque el OnModelCreating no lo mencione.
        await using (var probe = factory.CreateDbContext())
        {
            Assert.NotNull(probe.Model.FindEntityType(typeof(OutboxEntry)));
        }

        // El initializer crea la tabla del outbox.
        var initializer = new SqlOutboxTableInitializer<BusinessOnlyDbContext>(
            factory, null, NullLogger<SqlOutboxTableInitializer<BusinessOnlyDbContext>>.Instance);
        await initializer.InitializeAsync(CancellationToken.None);
        Assert.True(initializer.TableAvailable);

        // Escritura sin transaccion: se persiste de inmediato.
        var store = new SqlOutboxStore<BusinessOnlyDbContext>(factory.CreateDbContext(), factory, initializer);
        var write = await store.WriteAsync(new LogPayload { Content = "sin-tx" });
        Assert.Equal(WriteOutcome.Persisted, write.Outcome);
    }

    [Fact]
    public async Task TransactionalAtomicity_IsPreserved_WithInjectedOutboxEntity()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var factory = new Factory(connection);

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"BusinessRows\" (\"Id\" INTEGER NOT NULL CONSTRAINT \"PK_BusinessRows\" PRIMARY KEY AUTOINCREMENT, \"Name\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }

        var initializer = new SqlOutboxTableInitializer<BusinessOnlyDbContext>(
            factory, null, NullLogger<SqlOutboxTableInitializer<BusinessOnlyDbContext>>.Instance);
        await initializer.InitializeAsync(CancellationToken.None);
        Assert.True(initializer.TableAvailable);

        // Escenario ROLLBACK: negocio + log en la MISMA transaccion; al revertir, ninguno queda.
        await using (var consumer = factory.CreateDbContext())
        {
            var store = new SqlOutboxStore<BusinessOnlyDbContext>(consumer, factory, initializer);

            await using var tx = await consumer.Database.BeginTransactionAsync();

            consumer.Set<BusinessRow>().Add(new BusinessRow { Name = "pedido-rollback" });
            var w = await store.WriteAsync(new LogPayload { Content = "log-rollback" });
            Assert.Equal(WriteOutcome.Persisted, w.Outcome); // enlistado en el change tracker

            await consumer.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        // Tras el rollback: ni la fila de negocio ni el log sobrevivieron (atomicidad).
        await using (var check = factory.CreateDbContext())
        {
            Assert.Equal(0, await check.Set<BusinessRow>().CountAsync());
            Assert.Equal(0, await check.Set<OutboxEntry>().CountAsync());
        }

        // Escenario COMMIT: ambos se confirman juntos.
        await using (var consumer = factory.CreateDbContext())
        {
            var store = new SqlOutboxStore<BusinessOnlyDbContext>(consumer, factory, initializer);

            await using var tx = await consumer.Database.BeginTransactionAsync();

            consumer.Set<BusinessRow>().Add(new BusinessRow { Name = "pedido-commit" });
            await store.WriteAsync(new LogPayload { Content = "log-commit" });

            await consumer.SaveChangesAsync();
            await tx.CommitAsync();
        }

        await using (var check = factory.CreateDbContext())
        {
            Assert.Equal(1, await check.Set<BusinessRow>().CountAsync());
            Assert.Equal(1, await check.Set<OutboxEntry>().CountAsync());
        }
    }
}