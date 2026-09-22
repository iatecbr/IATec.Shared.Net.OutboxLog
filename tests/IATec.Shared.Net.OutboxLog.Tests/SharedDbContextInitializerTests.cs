using IATec.Shared.Net.OutboxLog;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Verifica que SqlOutboxTableInitializer crea SOLO la tabla del outbox cuando se apunta a un
/// DbContext de negocio que ya tiene otras tablas (escenario de una app existente). Usa SQLite en
/// memoria (relacional, sin servidor) para ejercitar la ruta real de creacion de tabla.
/// </summary>
public class SharedDbContextInitializerTests
{
    // Entidad de negocio ya existente en la app.
    public sealed class BusinessRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // DbContext COMPARTIDO: tablas de negocio + la entidad OutboxEntry del outbox.
    public sealed class SharedDbContext : DbContext
    {
        public SharedDbContext(DbContextOptions<SharedDbContext> options) : base(options) { }

        public DbSet<BusinessRow> BusinessRows => Set<BusinessRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<BusinessRow>().ToTable("BusinessRows");
            modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
        }
    }

    // Factory que reparte contextos sobre la MISMA conexion SQLite compartida (para :memory:).
    private sealed class SharedConnectionFactory : IDbContextFactory<SharedDbContext>
    {
        private readonly SqliteConnection _connection;
        public SharedConnectionFactory(SqliteConnection connection) => _connection = connection;

        public SharedDbContext CreateDbContext()
        {
            var options = new DbContextOptionsBuilder<SharedDbContext>()
                .UseSqlite(_connection)
                .Options;
            return new SharedDbContext(options);
        }
    }

    [Fact]
    public async Task Initializer_CreatesOnlyOutboxTable_OnExistingBusinessDatabase()
    {
        // Conexion SQLite en memoria mantenida abierta durante todo el test.
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();

        var factory = new SharedConnectionFactory(connection);

        // 1) La app ya tiene su esquema de negocio creado (simula migraciones previas): creamos SOLO
        //    la tabla de negocio a mano, dejando la del outbox inexistente.
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"BusinessRows\" (\"Id\" INTEGER NOT NULL CONSTRAINT \"PK_BusinessRows\" PRIMARY KEY AUTOINCREMENT, \"Name\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = "INSERT INTO \"BusinessRows\" (\"Name\") VALUES ('negocio-1');";
            await cmd.ExecuteNonQueryAsync();
        }

        // Sanidad: la tabla del outbox NO existe todavia.
        Assert.False(await TableExists(connection, "LogsOutboxEntries"));

        // 2) Ejecutar el initializer: debe crear SOLO LogsOutboxEntries.
        var initializer = new SqlOutboxTableInitializer<SharedDbContext>(
            factory,
            tableName: null,
            NullLogger<SqlOutboxTableInitializer<SharedDbContext>>.Instance);

        await initializer.InitializeAsync(CancellationToken.None);

        Assert.True(initializer.TableAvailable);
        Assert.True(await TableExists(connection, "LogsOutboxEntries"));

        // 3) La tabla y los datos de negocio quedaron intactos.
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM \"BusinessRows\";";
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            Assert.Equal(1, count);
        }

        // 4) El store puede escribir en la tabla recien creada.
        var store = new SqlOutboxStore<SharedDbContext>(
            factory.CreateDbContext(),
            factory,
            initializer);

        var result = await store.WriteAsync(new LogPayload
        {
            ContainerKey = "c",
            Source = "s",
            Owner = "o",
            Action = "a",
            UserId = "u",
            Content = "hola outbox",
        });

        Assert.Equal(WriteOutcome.Persisted, result.Outcome);

        // 5) Segunda inicializacion es idempotente: no falla ni recrea.
        await initializer.InitializeAsync(CancellationToken.None);
        Assert.True(initializer.TableAvailable);
    }

    private static async Task<bool> TableExists(SqliteConnection connection, string tableName)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=$name;";
        cmd.Parameters.AddWithValue("$name", tableName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
    }
}