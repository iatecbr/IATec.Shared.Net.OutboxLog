namespace IATec.Shared.Net.OutboxLog.WebApi.Models;

/// <summary>Cuerpo de una solicitud de log simple.</summary>
public sealed record LogRequest(string Message);

/// <summary>Cuerpo de una solicitud de log con contexto (scope) para enriquecer el payload.</summary>
public sealed record ScopedLogRequest(string Message, string Owner, string Action, string UserId);