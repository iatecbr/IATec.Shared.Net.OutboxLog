// Ejemplo de uso de la libreria IATec.Shared.Net.OutboxLog en una aplicacion externa.
//
// Esta app de consola usa el Generic Host de .NET, registra la libreria con el store
// EN MEMORIA (no requiere SQL Server ni Docker) y emite varios logs demostrando:
//   1. Registro/wiring via ILoggingBuilder.AddLogsOutbox(...)
//   2. Logging directo con el ILogger estandar (se captura automaticamente)
//   3. Enriquecimiento del payload con scopes (owner/action/userId)
//   4. Que el DispatchWorker corre en segundo plano y despacha las entradas
//   5. Apagado ordenado
//
// Para usar el store SQL Server en su lugar, vea el bloque comentado mas abajo.

using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

// --- Registro de la libreria (store en memoria) ---
// AddLogsOutbox sobre el ILoggingBuilder deja registrado el ILoggerProvider, el store,
// el cliente HTTP tipado y el DispatchWorker (hosted service) que despacha en segundo plano.
builder.Logging.AddLogsOutbox(options =>
{
    options.StoreType = OutboxStoreType.InMemory;

    // Endpoint del Log Bank. En el ejemplo apunta al valor por defecto; como no hay un
    // servidor real escuchando, las entregas se marcaran como no aceptadas y se reintentaran
    // con backoff (comportamiento esperado y visible en los logs de consola).
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";

    options.ContainerKey  = "sample-app";
    options.PollInterval  = TimeSpan.FromSeconds(2);
    options.BatchSize     = 50;
    options.RetryLimit    = 3;
    options.RequestTimeout = TimeSpan.FromSeconds(5);
});

// ------------------------------------------------------------------------------------------
// OPCION: store SQL Server durable (requiere connection string y EF Core). Descomentar y
// reemplazar el AddLogsOutbox de arriba. Necesita AppDbContext (definido al final del archivo).
//
// var cs = "Server=localhost;Database=Logs;Trusted_Connection=True;TrustServerCertificate=True";
// builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseSqlServer(cs));
// builder.Services.AddScoped(sp =>
//     new AppDbContext(new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(cs).Options));
// builder.Services.AddLogsOutbox<AppDbContext>(options =>
// {
//     options.StoreType      = OutboxStoreType.Sql;
//     options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
//     options.ContainerKey   = "sample-app";
//     options.PollInterval   = TimeSpan.FromSeconds(2);
// });
// ------------------------------------------------------------------------------------------

using IHost host = builder.Build();

// Iniciar el host arranca el DispatchWorker en segundo plano.
await host.StartAsync();

var logger = host.Services.GetRequiredService<ILoggerFactory>()
    .CreateLogger("IATec.Shared.Net.OutboxLog.Sample");

logger.LogInformation("Aplicacion de ejemplo iniciada");

// (1) Log simple: se captura y se persiste en el outbox automaticamente.
logger.LogInformation("Un log informativo de ejemplo");

// (2) Log con scope para enriquecer el payload (owner/action/userId).
using (logger.BeginScope(new Dictionary<string, object>
{
    ["owner"]  = "ventas1",
    ["action"] = "crear-pedido",
    ["userId"] = "user-123",
}))
{
    logger.LogInformation("Pedido creado para el cliente {ClienteId}", 42);
    logger.LogWarning("Stock bajo para el producto {ProductoId}", "SKU-999");
}

// (3) Log de error con excepcion (el detalle de la excepcion se incluye en el content).
try
{
    throw new InvalidOperationException("Fallo simulado de negocio");
}
catch (Exception ex)
{
    logger.LogError(ex, "Ocurrio un error procesando la solicitud");
}

// Dejar correr al worker unos segundos para que realice al menos un ciclo de despacho.
logger.LogInformation("Esperando a que el worker despache las entradas...");
await Task.Delay(TimeSpan.FromSeconds(6));

// Apagado ordenado: el worker deja de tomar lotes nuevos y espera la entrega en vuelo.
logger.LogInformation("Deteniendo la aplicacion de ejemplo");

Console.WriteLine("Ingrese valor");
string text = Console.ReadLine();

await host.StopAsync();

// ==========================================================================================
// DbContext de ejemplo para el modo SQL. Solo se usa si se descomenta el bloque SQL de arriba.
// Debe aplicar OutboxEntryConfiguration para mapear la entidad OutboxEntry (tabla
// "LogsOutboxEntries" por defecto).
// ==========================================================================================
// using Microsoft.EntityFrameworkCore;
//
// public sealed class AppDbContext : DbContext
// {
//     public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }
//
//     protected override void OnModelCreating(ModelBuilder modelBuilder)
//     {
//         base.OnModelCreating(modelBuilder);
//         modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
//     }
// }