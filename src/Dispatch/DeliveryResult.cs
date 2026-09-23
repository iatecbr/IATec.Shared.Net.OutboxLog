namespace IATec.Shared.Net.OutboxLog.Dispatch;

/// <summary>
/// Classifies the result of a single delivery attempt of a <see cref="LogPayload"/> to the
/// Log Bank Endpoint.
/// </summary>
public enum DeliveryOutcome
{
    /// <summary>The endpoint accepted the payload (an HTTP 2xx response).</summary>
    Accepted,

    /// <summary>The endpoint responded but did not accept the payload (a non-2xx response).</summary>
    Rejected,

    /// <summary>The request did not complete within the configured per-request timeout.</summary>
    TimedOut,

    /// <summary>The endpoint could not be reached (connection failure or transport error).</summary>
    Unreachable,
}

/// <summary>
/// The outcome of a single delivery attempt, together with the HTTP status code when the
/// endpoint produced a response.
/// </summary>
/// <param name="Outcome">The classified outcome of the attempt.</param>
/// <param name="StatusCode">
/// The HTTP status code returned by the endpoint, or <c>null</c> when no response was received
/// (timeout or unreachable endpoint).
/// </param>
public readonly record struct DeliveryResult(DeliveryOutcome Outcome, int? StatusCode);
