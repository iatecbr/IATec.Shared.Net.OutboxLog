namespace IATec.Shared.Net.OutboxLog.Configuration;

/// <summary>
/// Raised at registration time when the supplied <see cref="LogsOutboxOptions"/> are invalid.
/// Carries the name of the offending configuration field.
/// </summary>
public sealed class LogsOutboxConfigurationException : Exception
{
    /// <summary>The name of the configuration field that failed validation.</summary>
    public string FieldName { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="LogsOutboxConfigurationException"/> class.
    /// </summary>
    /// <param name="fieldName">The name of the offending configuration field.</param>
    /// <param name="message">A description of why the field is invalid.</param>
    public LogsOutboxConfigurationException(string fieldName, string message) : base(message)
        => FieldName = fieldName;
}
