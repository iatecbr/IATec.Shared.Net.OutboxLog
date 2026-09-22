using IATec.Shared.Net.OutboxLog;
using IATec.Shared.Net.OutboxLog.Dispatch;
using IATec.Shared.Net.OutboxLog.WebApi.Models;
using Microsoft.AspNetCore.Mvc;
using System.IO.IsolatedStorage;

namespace IATec.Shared.Net.OutboxLog.WebApi.Controllers;

/// <summary>
/// Endpoints de ejemplo. Algunos producen logs con el <see cref="ILogger"/> estandar (capturados
/// por la libreria via ILoggerProvider). Otros envian logs SIN usar ILogger, escribiendo
/// directamente al outbox (<see cref="IOutboxStore"/>) o enviando al Log Bank de forma inmediata
/// (<see cref="ILogBankClient"/>).
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class LogsController : ControllerBase
{
    private readonly ILogger<LogsController> _logger;
    private readonly IOutboxStore _outbox;
    private readonly ILogBankClient _logBankClient;

    public LogsController(
        ILogger<LogsController> logger,
        IOutboxStore outbox,
        ILogBankClient logBankClient)
    {
        _logger = logger;
        _outbox = outbox;
        _logBankClient = logBankClient;
    }

    /// <summary>Salud del servicio.</summary>
    [HttpGet("/health")]
    public IActionResult Health() => Ok(new { status = "ok" });

    /// <summary>
    /// Emite un log informativo. El payload se enriquece con las options de la libreria
    /// (ContainerKey/Source configurados en el arranque).
    /// </summary>
    [HttpPost("info")]
    public IActionResult Info([FromBody] LogRequest request)
    {
        Random random = new Random();                

        for (int i = 0; i < 1000; i++)
        {
            int randomNumber = random.Next(1, 100000);
            _logger.LogInformation("Evento informativo: {Message} {random}", request.Message, randomNumber);
        }

        
        return Accepted(new { captured = true, level = "Information", request.Message });
    }

    /// <summary>
    /// Emite un log dentro de un scope que rellena owner/action/userId del payload.
    /// </summary>
    [HttpPost("scoped")]
    public IActionResult Scoped([FromBody] ScopedLogRequest request)
    {
        using (_logger.BeginScope(new Dictionary<string, object>
        {
            ["owner"]  = request.Owner,
            ["action"] = request.Action,
            ["userId"] = request.UserId,
        }))
        {
            _logger.LogInformation("Evento con contexto: {Message}", request.Message);
        }

        return Accepted(new
        {
            captured = true,
            level = "Information",
            request.Owner,
            request.Action,
            request.UserId,
            request.Message
        });
    }

    /// <summary>
    /// Emite un log de error con excepcion (el detalle de la excepcion se incluye en el content).
    /// </summary>
    [HttpPost("error")]
    public IActionResult Error([FromBody] LogRequest request)
    {
        try
        {
            throw new InvalidOperationException(request.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error procesando la solicitud");
        }

        return Accepted(new { captured = true, level = "Error", request.Message });
    }

    /// <summary>
    /// Envia un log SIN usar ILogger, escribiendo directamente al outbox. El log se persiste y el
    /// DispatchWorker lo entrega en segundo plano (con reintentos y deduplicacion). Tu controlas
    /// todos los campos del payload de forma explicita.
    /// </summary>
    [HttpPost("outbox")]
    public async Task<IActionResult> WriteToOutbox([FromBody] ScopedLogRequest request, CancellationToken ct)
    {
        var payload = new LogPayload
        {
            ContainerKey = "webapi-sample",
            Source       = "webapi-service",
            Owner        = request.Owner,
            Action       = request.Action,
            UserId       = request.UserId,
            Content      = request.Message,
        };

        WriteResult result = await _outbox.WriteAsync(payload, ct);

        // Outcome: Persisted (nuevo), Deduplicated (ya existia una entrada con la misma clave) o Failed.
        return Accepted(new
        {
            via = "IOutboxStore",
            outcome = result.Outcome.ToString(),
            deduplicationKey = result.DeduplicationKey,
            success = result.Success,
            error = result.Error,
        });
    }

    /// <summary>
    /// Envia un log SIN usar ILogger y SIN outbox: hace el POST inmediato al Log Bank via
    /// ILogBankClient y devuelve el resultado de la entrega. No persiste ni reintenta.
    /// </summary>
    [HttpPost("direct")]
    public async Task<IActionResult> SendDirect([FromBody] ScopedLogRequest request, CancellationToken ct)
    {
        var payload = new LogPayload
        {
            ContainerKey = "webapi-sample",
            Source       = "webapi-service",
            Owner        = request.Owner,
            Action       = request.Action,
            UserId       = request.UserId,
            Content      = request.Message,
        };

        DeliveryResult result = await _logBankClient.SendAsync(payload, ct);

        var body = new
        {
            via = "ILogBankClient",
            outcome = result.Outcome.ToString(),
            statusCode = result.StatusCode,
        };

        // Accepted -> 200; cualquier otro resultado -> 502 para reflejar el fallo de entrega.
        return result.Outcome == DeliveryOutcome.Accepted
            ? Ok(body)
            : StatusCode(StatusCodes.Status502BadGateway, body);
    }
}