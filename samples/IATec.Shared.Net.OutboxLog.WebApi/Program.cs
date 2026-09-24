// Web API de ejemplo (.NET 10) que usa la libreria IATec.Shared.Net.OutboxLog.
//
// Usa CONTROLLERS tradicionales (no minimal API). Demuestra las tres formas de enviar logs:
//   1) Captura automatica via ILogger (opt-in: EnableLoggerProvider = true; por defecto false).
//   2) Envio explicito resiliente al outbox (IOutboxStore) -> endpoint POST /api/logs/outbox.
//   3) Envio directo e inmediato al Log Bank (ILogBankClient) -> endpoint POST /api/logs/direct.
//
// La entrega al Log Bank usa el cliente resiliente de IATec.Shared.HttpClient (circuit breaker,
// timeout y, opcionalmente, retry via Polly). Ver README.md en la raiz del repo para la guia
// completa de configuracion (store SQL/InMemory, circuit breaker, fallbacks de owner/action/userId,
// e integracion opcional con ILogger).

using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

//builder.Logging.AddLogsOutbox(options =>
//{
//    options.StoreType = OutboxStoreType.InMemory;
//    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
//    options.ContainerKey = "log-dispatcher-app";        // se aplica a cada log si no viene por scope
//    options.PollInterval = TimeSpan.FromSeconds(5);
//    options.RetryLimit = 3;
//    options.UserIdProvider = sp =>
//    {
//        var user = sp.GetService<IHttpContextAccessor>()?.HttpContext?.User;
//        var name = user?.Identity?.IsAuthenticated == true ? user.Identity.Name : null;
//        return string.IsNullOrEmpty(name) ? "N/A" : name;
//    };
//});

// Controllers tradicionales (MVC).
builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Necesario para que el UserIdProvider pueda leer el usuario autenticado de la request.
builder.Services.AddHttpContextAccessor();





// --- Registro de la libreria: store SQL Server durable ---
var cs = builder.Configuration.GetConnectionString("Default")!;
// AddLogsOutboxModel() inyecta la entidad OutboxEntry de forma transparente, sin tocar el
// OnModelCreating del AppDbContext de negocio. La atomicidad transaccional se conserva porque el
// log vive en el mismo context.
builder.Services.AddDbContextFactory<AppDbContext>(o => o
    .UseSqlServer(cs)
    .AddLogsOutboxModel());
builder.Services.AddScoped(sp =>
    new AppDbContext(new DbContextOptionsBuilder<AppDbContext>()
        .UseSqlServer(cs)
        .AddLogsOutboxModel()
        .Options));
builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
    options.ContainerKey = "log-dispatcher-app";

    // Opt-in: habilita la captura via ILogger (por defecto es false). Necesario para que los
    // endpoints /api/logs/info, /scoped y /error (que usan ILogger) lleguen al Log Bank.
    //options.EnableLoggerProvider = true;


    options.UserIdProvider = sp =>
    {
        var user = sp.GetService<IHttpContextAccessor>()?.HttpContext?.User;
        var name = user?.Identity?.IsAuthenticated == true ? user.Identity.Name : null;
        return string.IsNullOrEmpty(name) ? "N/A" : name;
    };
});






















// Nota sobre los fallbacks automaticos de la libreria cuando no hay scope:
//   owner  -> categoria del logger (la clase de contexto, p.ej. el tipo de ILogger<T>)
//   action -> EventId.Name o, en su defecto, el nivel de log (Information/Warning/Error/...)
//   userId -> options.UserIdProvider (arriba)

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();

// ==========================================================================================
// DbContext de negocio. No necesita saber nada del outbox: la entidad OutboxEntry se inyecta
// automaticamente via AddLogsOutboxModel() en el registro de arriba (Opcion 2). Aqui irian tus
// DbSet y tu configuracion de negocio.
// ==========================================================================================
public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    // protected override void OnModelCreating(ModelBuilder modelBuilder)
    // {
    //     base.OnModelCreating(modelBuilder);
    //     // ... tu configuracion de negocio ...
    // }
}