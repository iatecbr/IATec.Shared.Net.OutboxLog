# IATec.Shared.Net.OutboxLog

Reliable log delivery for .NET using the **Outbox** pattern. It captures logs (optionally via
`Microsoft.Extensions.Logging`), persists them in a local store (in-memory or any relational database supported by EF Core) and
dispatches them asynchronously to a centralized Log Bank with retries, deduplication and a
**circuit breaker**.

- Target: **.NET 10** (`net10.0`)
- Resilient HTTP client: **IATec.Shared.HttpClient** (Polly: retry, circuit breaker, timeout)

---

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [In-memory store vs relational database](#in-memory-store-vs-relational-database)
- [Ways to send logs](#ways-to-send-logs)
- [ILogger integration (optional)](#ilogger-integration-optional)
- [Payload enrichment (owner / action / userId)](#payload-enrichment-owner--action--userid)
- [Configuration reference](#configuration-reference)
- [Circuit breaker and resilience](#circuit-breaker-and-resilience)
- [How PollInterval, BatchSize and RetryLimit work](#how-pollinterval-batchsize-and-retrylimit-work)
- [Transactional atomicity (SQL store)](#transactional-atomicity-sql-store)

---

## Installation

The library references the `IATec.Shared.HttpClient` package from the `IATec.Community` feed. The
repository already includes a `nuget.config` with the required feeds (`nuget.org` + `IATec.Community`).

```xml
<PackageReference Include="IATec.Shared.Net.OutboxLog" Version="0.1.0" />
```

The consuming project must target **net10.0**.

---

## Quick start

Minimal registration with the in-memory store (no external dependencies):

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddLogsOutbox(options =>
{
    options.StoreType       = OutboxStoreType.InMemory;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
    options.ContainerKey    = "my-app";
    options.Source          = "my-service";
});

var app = builder.Build();
app.MapControllers();
app.Run();
```

This registers: the store, the `DispatchWorker` (background service that delivers in the background) and the
resilient HTTP client. Automatic capture via `ILogger` is **opt-in**: by default the `ILoggerProvider` is NOT
registered (see [ILogger integration](#ilogger-integration-optional)).

---

## In-memory store vs relational database

| Store | Durable | Requires | Use |
|---|---|---|---|
| `InMemory` | No (lost on restart) | nothing | tests, ephemeral workloads |
| `Sql` | Yes | EF Core + a relational provider + connection string | production, transactional atomicity |

> **Provider-agnostic.** The relational store works with **any database supported by EF Core**.
> The library only depends on `Microsoft.EntityFrameworkCore(.Relational)`; the concrete provider is
> supplied by your application:
>
> | Database | Provider package | Use |
> |---|---|---|
> | SQL Server | `Microsoft.EntityFrameworkCore.SqlServer` | `UseSqlServer(cs)` |
> | PostgreSQL | `Npgsql.EntityFrameworkCore.PostgreSQL` | `UseNpgsql(cs)` |
> | MySQL / MariaDB | `Pomelo.EntityFrameworkCore.MySql` | `UseMySql(cs, ...)` |
> | SQLite | `Microsoft.EntityFrameworkCore.Sqlite` | `UseSqlite(cs)` |

### Configuration with a relational database

The relational store requires you to register an `IDbContextFactory<TDbContext>` and a scoped
`TDbContext`, and to use the generic `AddLogsOutbox<TDbContext>` overload. The example uses SQL Server;
replace `UseSqlServer` with the `Use...` for your provider:

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.EntityFrameworkCore;

var cs = builder.Configuration.GetConnectionString("Default")!;

// SQL Server (or UseNpgsql / UseMySql / UseSqlite depending on your database)
builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseSqlServer(cs));
builder.Services.AddScoped(sp =>
    new AppDbContext(new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(cs).Options));

builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType       = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";
    options.ContainerKey    = "my-app";
    options.Source          = "my-service";
});
```

The `AppDbContext` must apply the configuration for the `OutboxEntry` entity:

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

At startup, a hosted service **silently creates the `LogsOutboxEntries` table** if it does not exist,
using EF Core (it generates the correct DDL for your provider). It is idempotent and **creates only the
outbox table**: it does not touch your other tables or your migrations. If the database is not available,
it **does not throw**: it logs the error and startup continues; the store is marked as unavailable and
`WriteAsync` returns `Failed` with `"outbox table unavailable"`.

### Integration into an app with an existing `DbContext`

You can reuse your own business `DbContext` (the one that already has your tables and migrations)
while preserving **transactional atomicity** (the log commits or rolls back together with your data).
There are two ways to map the outbox entity:

**Option A (recommended, without touching your `OnModelCreating`).** Call `AddLogsOutboxModel()` when
building the context options, after the provider. An `IModelCustomizer` transparently adds `OutboxEntry`
to the model:

```csharp
builder.Services.AddDbContextFactory<AppDbContext>(o => o
    .UseSqlServer(cs)
    .AddLogsOutboxModel());          // <- injects OutboxEntry without touching your DbContext

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

Your business `AppDbContext` **does not need to mention the outbox** at all:

```csharp
public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }
    public DbSet<Order> Orders => Set<Order>();   // only your business tables
}
```

**Option B (explicit).** If you prefer to map it yourself, apply the configuration in your
`OnModelCreating` and do not use `AddLogsOutboxModel()`:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
}
```

In either option:
- Register an `IDbContextFactory<AppDbContext>` (`AddDbContextFactory<AppDbContext>(...)`); the worker
  uses it for its reads/updates in a dedicated context.
- The initializer detects whether `LogsOutboxEntries` already exists and, if not, creates it at startup
  **without touching the rest of your schema**. If you manage the schema with migrations and already
  include that table, the initializer detects it as existing and does nothing.

---

## Ways to send logs

There are three ways for a log to reach the Log Bank:

### 1. Via `ILogger` (automatic capture)

With `EnableLoggerProvider = true` (opt-in; defaults to `false`), any log in the pipeline is captured:

```csharp
public class OrdersController(ILogger<OrdersController> logger)
{
    public IActionResult Create()
    {
        logger.LogInformation("Order created");
        return Ok();
    }
}
```

### 2. Via `IOutboxStore` (explicit, resilient)

Persists the log in the outbox; the worker delivers it in the background with retries and deduplication:

```csharp
using IATec.Shared.Net.OutboxLog;

public sealed class MyService(IOutboxStore outbox)
{
    public async Task RegisterAsync(CancellationToken ct)
    {
        var payload = new LogPayload
        {
            ContainerKey = "my-app",
            Source       = "my-service",
            Owner        = "sales",
            Action       = "create-order",
            UserId       = "user-123",
            Content      = "Manually built message",
        };

        WriteResult result = await outbox.WriteAsync(payload, ct);
        // result.Outcome: Persisted | Deduplicated | Failed
    }
}
```

### 3. Via `ILogBankClient` (direct, immediate send)

Performs the POST to the Log Bank right away, without persisting or retrying (the resilient client's
circuit breaker still applies):

```csharp
using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Dispatch;

public sealed class MyService(ILogBankClient client)
{
    public async Task SendAsync(CancellationToken ct)
    {
        var payload = new LogPayload { /* ... */ Content = "Direct send" };
        DeliveryResult result = await client.SendAsync(payload, ct);
        // result.Outcome: Accepted | Rejected | TimedOut | Unreachable
    }
}
```

| You need... | Use |
|---|---|
| Transparent capture of all logging | **1 — `ILogger`** |
| Not losing logs if the Log Bank goes down; retries; deduplication | **2 — `IOutboxStore`** |
| Send now and know instantly whether the API accepted | **3 — `ILogBankClient`** |

> In SQL mode, `IOutboxStore` is *scoped*: inject it inside a scope (a controller or a scoped
> service). `ILogBankClient` has no such restriction.

---

## ILogger integration (optional)

Automatic capture via `ILogger` is **opt-in and disabled by default**
(`EnableLoggerProvider = false`). By default the library does NOT register the `ILoggerProvider`: your
`logger.LogInformation(...)` calls are not captured and only the logs you send explicitly through
`IOutboxStore` or `ILogBankClient` reach the Log Bank. The store, the worker and the HTTP client are
registered regardless.

To **enable** automatic capture of the whole logging pipeline, set it to `true`:

```csharp
builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.StoreType = OutboxStoreType.Sql;
    options.LogBankEndpoint = "https://api-is-logs-dev.sdasystems.org/v1/log";

    // Opt-in: enables capture via ILogger (defaults to false).
    options.EnableLoggerProvider = true;
});
```

> When `ILogger` capture is active and you use the SQL store, it is advisable to avoid logging
> recursion (EF Core and the HttpClient also log). Filter by the `LogsOutbox` provider alias in
> `appsettings.json`:
>
> ```json
> {
>   "Logging": {
>     "LogLevel": { "Microsoft.EntityFrameworkCore": "Warning" },
>     "LogsOutbox": {
>       "LogLevel": { "Default": "None", "MyApp": "Information" }
>     }
>   }
> }
> ```
> This way only your application's categories (`MyApp`) are sent to the outbox.

---

## Payload enrichment (owner / action / userId)

The payload has the fields `containerKey`, `source`, `owner`, `action`, `userId`, `content`. To
populate them per log, use **scopes** with those exact keys (case-sensitive):

```csharp
using (logger.BeginScope(new Dictionary<string, object>
{
    ["owner"]  = "sales",
    ["action"] = "create-order",
    ["userId"] = "user-123",
}))
{
    logger.LogInformation("Order created");
}
```

### Mapping priority (and fallbacks)

The Log Bank requires that `owner`, `action` and `userId` are not empty. If there is no scope, the
library applies sensible fallbacks:

| Field | Priority |
|---|---|
| `containerKey` | scope `containerKey` → `options.ContainerKey` → `""` |
| `source` | scope `source` → `options.Source` → logger category → `""` |
| `owner` | scope `owner` → **logger category** (the context class) → `""` |
| `action` | scope `action` → `EventId.Name` → **log level** (`Information`, `Warning`, ...) → `""` |
| `userId` | scope `userId` → **`options.UserIdProvider`** → `""` |
| `content` | formatted message (+ exception detail), truncated to 8192 chars |

### `userId` from the authenticated user

`UserIdProvider` receives the app's `IServiceProvider`, so it can read the authenticated user:

```csharp
builder.Services.AddHttpContextAccessor();

builder.Services.AddLogsOutbox<AppDbContext>(options =>
{
    options.UserIdProvider = sp =>
    {
        var user = sp.GetService<IHttpContextAccessor>()?.HttpContext?.User;
        var name = user?.Identity?.IsAuthenticated == true ? user.Identity.Name : null;
        return string.IsNullOrEmpty(name) ? "N/A" : name; // "N/A" if anonymous
    };
});
```

---

## Configuration reference

`LogsOutboxOptions`:

| Option | Default | Range | Description |
|---|---|---|---|
| `LogBankEndpoint` | `https://api-is-logs-dev.sdasystems.org/v1/log` | absolute http/https URL | POST destination |
| `StoreType` | `InMemory` | `InMemory` / `Sql` | Persistence store |
| `EnableLoggerProvider` | `false` | bool | Registers (or not) the `ILoggerProvider`. Opt-in |
| `PollInterval` | `5s` | 1s–300s | Worker cadence |
| `BatchSize` | `100` | 1–1000 | Entries per cycle |
| `RetryLimit` | `3` | 1–10 | Attempts per entry before `Failed` |
| `RetryBackoff` | `30s` | 1s–3600s | Worker base backoff |
| `RequestTimeout` | `10s` | 1s–60s | Timeout per HTTP request |
| `UseCircuitBreaker` | `true` | bool | Enables the circuit breaker |
| `CircuitBreakerFailuresAllowedBeforeBreaking` | `4` | 1–100 | Consecutive failures before opening |
| `CircuitBreakerDuration` | `30s` | 1s–3600s | How long it stays open |
| `UseHttpRetry` | `false` | bool | Fast HTTP-level retries (separate from the worker) |
| `HttpRetryCount` | `0` | 0–10 | Number of HTTP retries if `UseHttpRetry` |
| `HttpRetryDelay` | `1s` | 0s–60s | Delay between HTTP retries |
| `ContainerKey` | `null` | — | Static payload value |
| `Source` | `null` | — | Static payload value |
| `UserIdProvider` | `null` | — | Delegate to resolve `userId` |

Notes:
- Configuration is validated at registration. An invalid value throws
  `LogsOutboxConfigurationException` with the field name, and leaves the container unchanged.
- With `StoreType.Sql` you must use the `AddLogsOutbox<TDbContext>` overload. The non-generic overload
  with `StoreType.Sql` is rejected with `FieldName = "DbContext"`.

---

## Circuit breaker and resilience

Delivery to the Log Bank uses the `IServiceClient` from `IATec.Shared.HttpClient` (Polly). The circuit
breaker prevents mass sending when the endpoint is unavailable or struggling.

**How it behaves when the Log Bank fails:**

1. After `CircuitBreakerFailuresAllowedBeforeBreaking` consecutive failures, the circuit **opens**.
2. With the circuit open, the client **does not hit the endpoint**: it returns `Unreachable` immediately.
3. The `DispatchWorker` leaves the entry `Pending` and reschedules it with backoff (up to `RetryLimit`).
4. After `CircuitBreakerDuration`, the circuit tries again; if it works, it closes.

**Two complementary levels of resilience:**

- **Circuit breaker + `RequestTimeout`** (within an attempt): protect against a down/slow endpoint.
- **`RetryLimit` + `RetryBackoff`** (over time, in the outbox): persistent retries that survive restarts.

`UseHttpRetry` defaults to `false` so as not to duplicate the worker's retries. Enable it only if you
also want immediate HTTP-level retries.

Circuit-breaker-only configuration (the rest at defaults):

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

## How PollInterval, BatchSize and RetryLimit work

The worker runs a loop: it processes a batch and then waits `PollInterval` before the next cycle.

- **`PollInterval`** — how often a cycle runs (default 5s). Lower = less latency but more load;
  higher = more efficient but logs take longer to go out.
- **`BatchSize`** — how many entries it processes per cycle (default 100), ordered from oldest to
  newest (FIFO by `CreatedAt`). If there are more pending, they are processed in later cycles.
- **`RetryLimit`** — delivery attempts per entry (default 3). On each failure the counter is
  incremented and it is rescheduled with exponential backoff; upon reaching the limit, the entry is
  marked `Failed` and stops being retried.

When a delivery is `Accepted`, the entry is **removed** from the store (no tombstone is left). Only
those that exhaust their retries remain as `Failed`.

---

## Transactional atomicity (SQL store)

If you write a log inside an open transaction on your `DbContext`, the entry is enlisted in the same
change tracker and commits or rolls back together with your business data:

```csharp
await using var tx = await db.Database.BeginTransactionAsync();

db.Orders.Add(newOrder);
await outbox.WriteAsync(payload); // enlisted, not yet visible to the worker

await db.SaveChangesAsync();
await tx.CommitAsync();   // now the log becomes available for dispatch
// tx.Rollback() -> the log is not persisted either
```

Without an active transaction, the write is persisted immediately. A duplicate log (same deduplication
key) never breaks your business transaction: it is deduplicated before being enlisted.
