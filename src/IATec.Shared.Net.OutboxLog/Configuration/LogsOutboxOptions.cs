namespace IATec.Shared.Net.OutboxLog.Configuration;

/// <summary>
/// Configuration options for the Logs Outbox library. Validated at registration time.
/// </summary>
public sealed class LogsOutboxOptions
{
    /// <summary>Base URL of the Log Bank Endpoint. Default: https://api-is-logs-dev.sdasystems.org/v1/log</summary>
    public string LogBankEndpoint { get; set; } = "https://api-is-logs-dev.sdasystems.org/v1/log";

    /// <summary>Max delivery attempts per entry. Range 1..10 inclusive. Default 3.</summary>
    public int RetryLimit { get; set; } = 3;

    /// <summary>Which store implementation to use. Default InMemory.</summary>
    public OutboxStoreType StoreType { get; set; } = OutboxStoreType.InMemory;

    /// <summary>
    /// Cuando es true la libreria se integra con Microsoft.Extensions.Logging registrando un
    /// <c>ILoggerProvider</c>, de modo que todo log del pipeline se captura y se envia al Log Bank.
    /// Cuando es false (por defecto) NO se registra el provider: la captura via ILogger queda
    /// desactivada y solo puedes enviar logs explicitamente usando <c>IOutboxStore</c> o
    /// <c>ILogBankClient</c>. El store, el worker de despacho y el cliente HTTP se registran igual
    /// en ambos casos. Es opt-in: actívalo cuando quieras la captura automatica del pipeline.
    /// </summary>
    public bool EnableLoggerProvider { get; set; } = false;

    /// <summary>Polling cycle interval. Range 1s..300s. Default 5s.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Max entries returned per poll. Range 1..1000. Default 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Per-attempt HTTP request timeout. Range 1s..60s. Default 10s.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Base retry backoff. Range 1s..3600s. Default 30s.</summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromSeconds(30);

    // ----- Politica de resiliencia HTTP (IATec.Shared.HttpClient / Polly) -----
    // Estas opciones controlan el IServiceClient que entrega los payloads al Log Bank. El circuit
    // breaker evita el envio masivo cuando el endpoint no esta disponible o con dificultades: tras
    // varios fallos consecutivos abre el circuito y rechaza envios durante un periodo, dejando que
    // el worker reintente mas tarde con su propio backoff.

    /// <summary>
    /// Habilita el circuit breaker del cliente HTTP. Cuando esta abierto, las entregas se rechazan
    /// de inmediato (mapeadas a <c>Unreachable</c>) sin golpear el endpoint. Default true.
    /// </summary>
    public bool UseCircuitBreaker { get; set; } = true;

    /// <summary>
    /// Numero de fallos consecutivos permitidos antes de abrir el circuito. Rango 1..100. Default 4.
    /// </summary>
    public int CircuitBreakerFailuresAllowedBeforeBreaking { get; set; } = 4;

    /// <summary>
    /// Tiempo que el circuito permanece abierto antes de volver a permitir intentos. Rango
    /// 1s..3600s. Default 30s.
    /// </summary>
    public TimeSpan CircuitBreakerDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Habilita reintentos rapidos dentro de un mismo intento de entrega (independientes del
    /// RetryLimit/backoff del worker). Default false para no duplicar la semantica de reintentos
    /// del outbox; actívelo solo si quiere reintentos inmediatos ademas de los del worker.
    /// </summary>
    public bool UseHttpRetry { get; set; } = false;

    /// <summary>Numero de reintentos rapidos del cliente HTTP cuando UseHttpRetry es true. Rango 0..10. Default 0.</summary>
    public int HttpRetryCount { get; set; } = 0;

    /// <summary>Retardo entre reintentos rapidos del cliente HTTP. Rango 0s..60s. Default 1s.</summary>
    public TimeSpan HttpRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Static container key used to populate the payload when not supplied per-log.</summary>
    public string? ContainerKey { get; set; }

    /// <summary>Static source used to populate the payload when not supplied per-log.</summary>
    public string? Source { get; set; }

    /// <summary>
    /// Optional provider that resolves the current user id for the payload's <c>userId</c> field
    /// when no <c>userId</c> scope value is supplied. Receives the application's
    /// <see cref="IServiceProvider"/> so it can resolve request-scoped services (for example, an
    /// <c>IHttpContextAccessor</c> to read <c>HttpContext.User.Identity.Name</c>). Must never throw;
    /// any exception is swallowed and treated as "no user". Returning null or empty falls back to
    /// the empty string.
    /// </summary>
    public Func<IServiceProvider, string?>? UserIdProvider { get; set; }
}
