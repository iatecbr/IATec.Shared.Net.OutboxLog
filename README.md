# IATec.Shared.Net.OutboxLog

Entrega confiable de logs para .NET usando el patrón **Outbox**. Captura logs (opcionalmente vía
`Microsoft.Extensions.Logging`), los persiste en un store local (en memoria o en cualquier base de datos relacional soportada por EF Core) y los
despacha de forma asíncrona a un Log Bank centralizado con reintentos, deduplicación y
**circuit breaker**.

- Target: **.NET 10** (`net10.0`)
- Cliente HTTP resiliente: **IATec.Shared.HttpClient** (Polly: retry, circuit breaker, timeout)

---

## Índice

- [Instalación](#instalación)
- [Inicio rápido](#inicio-rápido)
- [Store en memoria vs base de datos relacional](#store-en-memoria-vs-base-de-datos-relacional)
- [Formas de enviar logs](#formas-de-enviar-logs)
- [Integración con ILogger (opcional)](#integración-con-ilogger-opcional)
- [Enriquecimiento del payload (owner / action / userId)](#enriquecimiento-del-payload-owner--action--userid)
- [Referencia de configuración](#referencia-de-configuración)
- [Circuit breaker y resiliencia](#circuit-breaker-y-resiliencia)
- [Cómo funcionan PollInterval, BatchSize y RetryLimit](#cómo-funcionan-pollinterval-batchsize-y-retrylimit)
- [Atomicidad transaccional (store SQL)](#atomicidad-transaccional-store-sql)

---

## Instalación

La librería referencia el paquete `IATec.Shared.HttpClient` del feed `IATec.Community`. El
repositorio ya incluye un `nuget.config` con los feeds necesarios (`nuget.org` + `IATec.Community`).

```xml
<PackageReference Include="IATec.Shared.Net.OutboxLog" Version="0.1.0" />
```

El proyecto consumidor debe apuntar a **net10.0**.

---

## Inicio rápido

Registro mínimo con store en memoria (sin dependencias externas):

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddLogsOutbox(options =>
{
    options.StoreType       = OutboxStoreType.InMemory;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
    options.ContainerKey    = "mi-app";
    options.Source          = "mi-servicio";
});

var app = builder.Build();
app.MapControllers();
app.Run();
```

Esto registra: el store, el `DispatchWorker` (background service que entrega en segundo plano) y el
cliente HTTP resiliente. La captura automática vía `ILogger` es **opt-in**: por defecto NO se
registra el `ILoggerProvider` (ver [Integración con ILogger](#integración-con-ilogger-opcional)).

---

## Store en memoria vs base de datos relacional

| Store | Durable | Requiere | Uso |
|---|---|---|---|
| `InMemory` | No (se pierde al reiniciar) | nada | pruebas, cargas efímeras |
| `Sql` | Sí | EF Core + un provider relacional + connection string | producción, atomicidad transaccional |

> **Agnóstico de proveedor.** El store relacional funciona con **cualquier base de datos soportada
> por EF Core**. La librería solo depende de `Microsoft.EntityFrameworkCore(.Relational)`; el
> provider concreto lo aporta tu aplicación:
>
> | Base de datos | Paquete del provider | Uso |
> |---|---|---|
> | SQL Server | `Microsoft.EntityFrameworkCore.SqlServer` | `UseSqlServer(cs)` |
> | PostgreSQL | `Npgsql.EntityFrameworkCore.PostgreSQL` | `UseNpgsql(cs)` |
> | MySQL / MariaDB | `Pomelo.EntityFrameworkCore.MySql` | `UseMySql(cs, ...)` |
> | SQLite | `Microsoft.EntityFrameworkCore.Sqlite` | `UseSqlite(cs)` |

### Configuración con base de datos relacional

El store relacional necesita que registres un `IDbContextFactory<TDbContext>` y un `TDbContext`
scoped, y que uses el overload genérico `AddLogsOutbox<TDbContext>`. El ejemplo usa SQL Server;
cambia `UseSqlServer` por el `Use...` de tu provider:

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.EntityFrameworkCore;

var cs = builder.Configuration.GetConnectionString("Default")!;

// SQL Server (o UseNpgsql / UseMySql / UseSqlite segun tu base)
builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseSqlServer(cs));
builder.Services.AddScoped(sp =>
    new AppDbContext(new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(cs).Options));

builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType       = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
    options.ContainerKey    = "mi-app";
    options.Source          = "mi-servicio";
});
```

El `AppDbContext` debe aplicar la configuración de la entidad `OutboxEntry`:

```csharp
using IATec.Shared.Net.OutboxLog;
using Microsoft.EntityFrameworkCore;

public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
    }
}
```

Al arrancar, un hosted service crea **silenciosamente la tabla `LogsOutboxEntries`** si no existe,
usando EF Core (genera el DDL correcto para tu provider). Es idempotente y **crea solo la tabla del
outbox**: no toca tus demás tablas ni tus migraciones. Si la base no está disponible, **no lanza**:
registra el error y el arranque continúa; el store queda marcado como no disponible y `WriteAsync`
devuelve `Failed` con `"outbox table unavailable"`.

### Integración en una app con un `DbContext` ya existente

Puedes reutilizar tu propio `DbContext` de negocio (el que ya tiene tus tablas y migraciones)
conservando la **atomicidad transaccional** (el log se confirma o revierte junto con tus datos).
Hay dos formas de mapear la entidad del outbox:

**Forma A (recomendada, sin tocar tu `OnModelCreating`).** Llama a `AddLogsOutboxModel()` al
construir las opciones del context, después del provider. Un `IModelCustomizer` añade `OutboxEntry`
al modelo de forma transparente:

```csharp
builder.Services.AddDbContextFactory<AppDbContext>(o => o
    .UseSqlServer(cs)
    .AddLogsOutboxModel());          // <- inyecta OutboxEntry sin tocar tu DbContext

builder.Services.AddScoped(sp =>
    new AppDbContext(new DbContextOptionsBuilder<AppDbContext>()
        .UseSqlServer(cs)
        .AddLogsOutboxModel()
        .Options));

builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
});
```

Tu `AppDbContext` de negocio **no necesita mencionar el outbox** para nada:

```csharp
public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }
    public DbSet<Pedido> Pedidos => Set<Pedido>();   // solo tus tablas de negocio
}
```

**Forma B (explícita).** Si prefieres mapearlo tú mismo, aplica la configuración en tu
`OnModelCreating` y no uses `AddLogsOutboxModel()`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
}
```

En cualquiera de las dos formas:
- Registra un `IDbContextFactory<AppDbContext>` (`AddDbContextFactory<AppDbContext>(...)`); el worker
  lo usa para sus lecturas/actualizaciones en un context dedicado.
- El initializer detecta si `LogsOutboxEntries` ya existe y, si no, la crea al arrancar **sin tocar
  el resto de tu esquema**. Si gestionas el esquema con migraciones y ya incluyes esa tabla, el
  initializer la detecta como existente y no hace nada.

---

## Formas de enviar logs

Hay tres formas de que un log llegue al Log Bank:

### 1. Vía `ILogger` (captura automática)

Con `EnableLoggerProvider = true` (opt-in; por defecto es `false`), cualquier log del pipeline se captura:

```csharp
public class PedidosController(ILogger<PedidosController> logger)
{
    public IActionResult Crear()
    {
        logger.LogInformation("Pedido creado");
        return Ok();
    }
}
```

### 2. Vía `IOutboxStore` (explícito, resiliente)

Persiste el log en el outbox; el worker lo entrega en segundo plano con reintentos y deduplicación:

```csharp
using IATec.Shared.Net.OutboxLog;

public sealed class MiServicio(IOutboxStore outbox)
{
    public async Task RegistrarAsync(CancellationToken ct)
    {
        var payload = new LogPayload
        {
            ContainerKey = "mi-app",
            Source       = "mi-servicio",
            Owner        = "ventas",
            Action       = "crear-pedido",
            UserId       = "user-123",
            Content      = "Mensaje construido manualmente",
        };

        WriteResult result = await outbox.WriteAsync(payload, ct);
        // result.Outcome: Persisted | Deduplicated | Failed
    }
}
```

### 3. Vía `ILogBankClient` (envío directo e inmediato)

Hace el POST al Log Bank en el momento, sin persistir ni reintentar (aplica igual el circuit
breaker del cliente resiliente):

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Dispatch;

public sealed class MiServicio(ILogBankClient client)
{
    public async Task EnviarAsync(CancellationToken ct)
    {
        var payload = new LogPayload { /* ... */ Content = "Envío directo" };
        DeliveryResult result = await client.SendAsync(payload, ct);
        // result.Outcome: Accepted | Rejected | TimedOut | Unreachable
    }
}
```

| Necesitas... | Usa |
|---|---|
| Captura transparente de todo el logging | **1 — `ILogger`** |
| No perder logs si el Log Bank cae; reintentos; deduplicación | **2 — `IOutboxStore`** |
| Enviar ahora y saber al instante si la API aceptó | **3 — `ILogBankClient`** |

> En modo SQL, `IOutboxStore` es *scoped*: inyéctalo dentro de un scope (un controller o servicio
> scoped). `ILogBankClient` no tiene esa restricción.

---

## Integración con ILogger (opcional)

La captura automática vía `ILogger` es **opt-in y está desactivada por defecto**
(`EnableLoggerProvider = false`). Por defecto la librería NO registra el `ILoggerProvider`: tus
`logger.LogInformation(...)` no se capturan y solo llegan al Log Bank los logs que envíes
explícitamente por `IOutboxStore` o `ILogBankClient`. El store, el worker y el cliente HTTP se
registran igual.

Para **activar** la captura automática de todo el pipeline de logging, ponla en `true`:

```csharp
builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";

    // Opt-in: habilita la captura por ILogger (por defecto false).
    options.EnableLoggerProvider = true;
});
```

> Cuando la captura por `ILogger` está activa y usas el store SQL, conviene evitar la recursión de
> logging (EF Core y el HttpClient también loguean). Filtra por el alias del provider `LogsOutbox`
> en `appsettings.json`:
>
> ```json
> {
>   "Logging": {
>     "LogLevel": { "Microsoft.EntityFrameworkCore": "Warning" },
>     "LogsOutbox": {
>       "LogLevel": { "Default": "None", "MiApp": "Information" }
>     }
>   }
> }
> ```
> Así solo las categorías de tu aplicación (`MiApp`) se envían al outbox.

---

## Enriquecimiento del payload (owner / action / userId)

El payload tiene los campos `containerKey`, `source`, `owner`, `action`, `userId`, `content`. Para
rellenarlos por log usa **scopes** con esas claves exactas (case-sensitive):

```csharp
using (logger.BeginScope(new Dictionary<string, object>
{
    ["owner"]  = "ventas",
    ["action"] = "crear-pedido",
    ["userId"] = "user-123",
}))
{
    logger.LogInformation("Pedido creado");
}
```

### Prioridad de mapeo (y fallbacks)

El Log Bank exige que `owner`, `action` y `userId` no vayan vacíos. Si no hay scope, la librería
aplica fallbacks con sentido:

| Campo | Prioridad |
|---|---|
| `containerKey` | scope `containerKey` → `options.ContainerKey` → `""` |
| `source` | scope `source` → `options.Source` → categoría del logger → `""` |
| `owner` | scope `owner` → **categoría del logger** (la clase de contexto) → `""` |
| `action` | scope `action` → `EventId.Name` → **nivel de log** (`Information`, `Warning`, ...) → `""` |
| `userId` | scope `userId` → **`options.UserIdProvider`** → `""` |
| `content` | mensaje formateado (+ detalle de excepción), truncado a 8192 chars |

### `userId` desde el usuario autenticado

`UserIdProvider` recibe el `IServiceProvider` de la app, así que puede leer el usuario autenticado:

```csharp
builder.Services.AddHttpContextAccessor();

builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.UserIdProvider = sp =>
    {
        var user = sp.GetService<IHttpContextAccessor>()?.HttpContext?.User;
        var name = user?.Identity?.IsAuthenticated == true ? user.Identity.Name : null;
        return string.IsNullOrEmpty(name) ? "N/A" : name; // "N/A" si es anónimo
    };
});
```

---

## Referencia de configuración

`LogsOutboxOptions`:

| Opción | Default | Rango | Descripción |
|---|---|---|---|
| `LogBankEndpoint` | `https://api-is-logs-dev.sdasystems.org/v1/log` | URL http/https absoluta | Destino del POST |
| `StoreType` | `InMemory` | `InMemory` / `Sql` | Store de persistencia |
| `EnableLoggerProvider` | `false` | bool | Registra (o no) el `ILoggerProvider`. Opt-in |
| `PollInterval` | `5s` | 1s–300s | Cadencia del worker |
| `BatchSize` | `100` | 1–1000 | Entradas por ciclo |
| `RetryLimit` | `3` | 1–10 | Intentos por entrada antes de `Failed` |
| `RetryBackoff` | `30s` | 1s–3600s | Backoff base del worker |
| `RequestTimeout` | `10s` | 1s–60s | Timeout por request HTTP |
| `UseCircuitBreaker` | `true` | bool | Activa el circuit breaker |
| `CircuitBreakerFailuresAllowedBeforeBreaking` | `4` | 1–100 | Fallos consecutivos antes de abrir |
| `CircuitBreakerDuration` | `30s` | 1s–3600s | Cuánto permanece abierto |
| `UseHttpRetry` | `false` | bool | Reintentos rápidos a nivel HTTP (aparte del worker) |
| `HttpRetryCount` | `0` | 0–10 | Nº de reintentos HTTP si `UseHttpRetry` |
| `HttpRetryDelay` | `1s` | 0s–60s | Retardo entre reintentos HTTP |
| `ContainerKey` | `null` | — | Valor estático del payload |
| `Source` | `null` | — | Valor estático del payload |
| `UserIdProvider` | `null` | — | Delegado para resolver `userId` |

Notas:
- La configuración se valida en el registro. Un valor inválido lanza
  `LogsOutboxConfigurationException` con el nombre del campo, y deja el contenedor sin modificar.
- Con `StoreType.Sql` debes usar el overload `AddLogsOutbox<TDbContext>`. El overload no genérico
  con `StoreType.Sql` se rechaza con `FieldName = "DbContext"`.

---

## Circuit breaker y resiliencia

La entrega al Log Bank usa el `IServiceClient` de `IATec.Shared.HttpClient` (Polly). El circuit
breaker evita el envío masivo cuando el endpoint no está disponible o con dificultades.

**Cómo se comporta cuando el Log Bank falla:**

1. Tras `CircuitBreakerFailuresAllowedBeforeBreaking` fallos consecutivos, el circuito se **abre**.
2. Con el circuito abierto, el cliente **no golpea el endpoint**: devuelve `Unreachable` de inmediato.
3. El `DispatchWorker` deja la entrada `Pending` y la reprograma con backoff (hasta `RetryLimit`).
4. Pasada `CircuitBreakerDuration`, el circuito prueba de nuevo; si funciona, se cierra.

**Dos niveles de resiliencia que se complementan:**

- **Circuit breaker + `RequestTimeout`** (dentro de un intento): protegen contra endpoint caído/lento.
- **`RetryLimit` + `RetryBackoff`** (a lo largo del tiempo, en el outbox): reintentos persistentes
  que sobreviven a reinicios.

`UseHttpRetry` viene en `false` para no duplicar los reintentos del worker. Actívalo solo si además
quieres reintentos inmediatos a nivel HTTP.

Configuración solo del circuit breaker (el resto en defaults):

```csharp
builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType = OutboxStoreType.Sql;
    options.UseCircuitBreaker = true;
    options.CircuitBreakerFailuresAllowedBeforeBreaking = 4;
    options.CircuitBreakerDuration = TimeSpan.FromSeconds(30);
});
```

---

## Cómo funcionan PollInterval, BatchSize y RetryLimit

El worker corre un bucle: procesa un lote y luego espera `PollInterval` antes del siguiente ciclo.

- **`PollInterval`** — cada cuánto se ejecuta un ciclo (default 5s). Menor = menos latencia pero más
  carga; mayor = más eficiente pero los logs tardan más en salir.
- **`BatchSize`** — cuántas entradas procesa por ciclo (default 100), ordenadas de más antigua a más
  nueva (FIFO por `CreatedAt`). Si hay más pendientes, se procesan en ciclos siguientes.
- **`RetryLimit`** — intentos de entrega por entrada (default 3). En cada fallo se incrementa el
  contador y se reprograma con backoff exponencial; al alcanzar el límite, la entrada se marca
  `Failed` y deja de reintentarse.

Cuando una entrega es `Accepted`, la entrada se **elimina** del store (no queda como tombstone).
Solo las que agotan reintentos permanecen como `Failed`.

---

## Atomicidad transaccional (store SQL)

Si escribes un log dentro de una transacción abierta en tu `DbContext`, la entrada se enlista en el
mismo change tracker y se confirma o revierte junto con tus datos de negocio:

```csharp
await using var tx = await db.Database.BeginTransactionAsync();

db.Pedidos.Add(nuevoPedido);
await outbox.WriteAsync(payload); // se enlista, aún no visible al worker

await db.SaveChangesAsync();
await tx.CommitAsync();   // ahora el log queda disponible para despacho
// tx.Rollback() -> el log tampoco se persiste
```

Sin transacción activa, la escritura se persiste de inmediato. Un log duplicado (misma
deduplication key) nunca rompe tu transacción de negocio: se deduplica antes de enlistarse.