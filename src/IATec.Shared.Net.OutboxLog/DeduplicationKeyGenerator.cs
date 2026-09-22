using System.Security.Cryptography;
using System.Text;

namespace IATec.Shared.Net.OutboxLog;

/// <summary>
/// Produces the deterministic deduplication key for a <see cref="LogPayload"/>.
/// </summary>
/// <remarks>
/// The key is a SHA-256 hash, lowercase hex-encoded, computed over a canonical
/// serialization of the payload fields in a fixed order
/// (<c>ContainerKey</c>, <c>Source</c>, <c>Owner</c>, <c>Action</c>, <c>UserId</c>, <c>Content</c>).
/// Each field is length-prefixed so that no combination of delimiter characters in the
/// field values can produce a collision (i.e. the serialization is unambiguous). This makes
/// the key deterministic and content-discriminating: identical content yields the same key,
/// and any difference in canonical content yields a different key (Requirement 6.1).
/// </remarks>
public static class DeduplicationKeyGenerator
{
    /// <summary>
    /// Computes the deterministic deduplication key for the supplied payload.
    /// </summary>
    /// <param name="payload">The log payload to derive the key from.</param>
    /// <returns>A lowercase hex-encoded SHA-256 hash of the canonical payload serialization.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="payload"/> is null.</exception>
    public static string Compute(LogPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        // Fixed field order. Length-prefixing each field guarantees an unambiguous,
        // delimiter-collision-free encoding of the tuple of field values.
        var builder = new StringBuilder();
        AppendField(builder, payload.ContainerKey);
        AppendField(builder, payload.Source);
        AppendField(builder, payload.Owner);
        AppendField(builder, payload.Action);
        AppendField(builder, payload.UserId);
        AppendField(builder, payload.Content);

        var canonicalBytes = Encoding.UTF8.GetBytes(builder.ToString());
        var hash = SHA256.HashData(canonicalBytes);

        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Appends a single field to the canonical buffer as its UTF-8 byte length,
    /// a separator, and the raw value. The byte length (rather than character length)
    /// is used so multi-byte characters cannot shift field boundaries.
    /// </summary>
    private static void AppendField(StringBuilder builder, string? value)
    {
        value ??= string.Empty;
        var byteLength = Encoding.UTF8.GetByteCount(value);
        builder.Append(byteLength);
        builder.Append(':');
        builder.Append(value);
    }
}
