using System.Text.Json.Serialization;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// The wire model sent to the Log Bank Endpoint, serialized as JSON with camelCase names.
/// Required fields default to the empty string when no value is available.
/// </summary>
public sealed record LogPayload
{
    /// <summary>Identifier of the logical container the log belongs to.</summary>
    [JsonPropertyName("containerKey")]
    public string ContainerKey { get; init; } = "";

    /// <summary>Origin of the log event (e.g. logger category).</summary>
    [JsonPropertyName("source")]
    public string Source { get; init; } = "";

    /// <summary>Owner associated with the log event.</summary>
    [JsonPropertyName("owner")]
    public string Owner { get; init; } = "";

    /// <summary>Action associated with the log event.</summary>
    [JsonPropertyName("action")]
    public string Action { get; init; } = "";

    /// <summary>Optional user identifier associated with the log event.</summary>
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";

    /// <summary>The formatted log content. Truncated to a maximum of 8192 characters.</summary>
    [JsonPropertyName("content")]
    public string Content { get; init; } = "";
}
