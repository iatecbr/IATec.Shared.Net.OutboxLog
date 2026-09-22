using CsCheck;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.Tests;

/// <summary>
/// Property-based tests for <see cref="DeduplicationKeyGenerator"/>.
/// </summary>
public class DeduplicationKeyGeneratorPropertyTests
{
    /// <summary>
    /// Generates payload field values, deliberately including edge cases:
    /// empty and whitespace strings, Unicode, control characters, and the ':'
    /// delimiter used by the canonical serialization (to guard against collisions).
    /// </summary>
    private static readonly Gen<string> GenField =
        Gen.OneOf(
            Gen.Const(""),
            Gen.Const(" "),
            Gen.Const(":"),
            Gen.Const("0:"),
            Gen.Const("\u0000"),
            Gen.Const("héllo-\u00e9\u4e2d\u6587"),
            Gen.String);

    private static readonly Gen<LogPayload> GenPayload =
        Gen.Select(GenField, GenField, GenField, GenField, GenField, GenField,
            (containerKey, source, owner, action, userId, content) => new LogPayload
            {
                ContainerKey = containerKey,
                Source = source,
                Owner = owner,
                Action = action,
                UserId = userId,
                Content = content,
            });

    // Feature: logs-outbox-library, Property 1: Deduplication key is deterministic and content-discriminating
    // Validates: Requirements 6.1
    [Fact]
    public void DeduplicationKey_IsDeterministic_And_ContentDiscriminating()
    {
        // Part A: identical content always yields the same key (determinism).
        GenPayload.Sample(payload =>
        {
            // A separate but content-identical instance must produce the same key.
            var clone = payload with { };
            var key1 = DeduplicationKeyGenerator.Compute(payload);
            var key2 = DeduplicationKeyGenerator.Compute(clone);

            Assert.Equal(key1, key2);
            Assert.False(string.IsNullOrEmpty(key1));
        }, iter: 100);

        // Part B: payloads differing in canonical content yield different keys.
        // Generate a pair, then force a difference in at least one field so the
        // canonical content genuinely differs, and assert the keys differ.
        Gen.Select(GenPayload, GenPayload)
            .Sample(pair =>
            {
                var (a, b) = pair;

                // Ensure the two payloads have genuinely different canonical content.
                // If the generator happened to produce equal payloads, mutate one field.
                if (a == b)
                {
                    b = b with { Content = b.Content + "\u0001diff" };
                }

                var keyA = DeduplicationKeyGenerator.Compute(a);
                var keyB = DeduplicationKeyGenerator.Compute(b);

                Assert.NotEqual(keyA, keyB);
            }, iter: 100);
    }

    // Feature: logs-outbox-library, Property 1: Deduplication key is deterministic and content-discriminating
    // Validates: Requirements 6.1
    [Fact]
    public void DeduplicationKey_DiffersWhenAnySingleFieldDiffers()
    {
        // For every field, changing only that field must change the key. This is the
        // "content-discriminating" guarantee at the granularity of individual fields,
        // and also exercises the length-prefixing that prevents delimiter collisions.
        GenPayload.Sample(payload =>
            {
                var baseKey = DeduplicationKeyGenerator.Compute(payload);

                // Try mutating each field; use a marker guaranteed to change the value.
                LogPayload[] variants =
                [
                    payload with { ContainerKey = payload.ContainerKey + "\u0002x" },
                    payload with { Source = payload.Source + "\u0002x" },
                    payload with { Owner = payload.Owner + "\u0002x" },
                    payload with { Action = payload.Action + "\u0002x" },
                    payload with { UserId = payload.UserId + "\u0002x" },
                    payload with { Content = payload.Content + "\u0002x" },
                ];

                foreach (var variant in variants)
                {
                    Assert.NotEqual(baseKey, DeduplicationKeyGenerator.Compute(variant));
                }
            }, iter: 100);
    }
}
