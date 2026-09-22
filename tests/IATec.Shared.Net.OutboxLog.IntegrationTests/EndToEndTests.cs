// Feature: logs-outbox-library
// End-to-end integration test: capture -> persist -> POST (stub HTTP endpoint) -> mark-delivered.
// Validates: Requirements 1.2 (DI wiring resolves the provider, store, and worker) and
//            5.3 (an accepted delivery marks the entry Delivered), exercised end to end
//            against a real SQL Server instance via Testcontainers.
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using IATec.Shared.Net.OutboxLog.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IATec.Shared.Net.OutboxLog.IntegrationTests;

/// <summary>
/// Exercises the entire library through its real DI wiring against a real database:
/// the standard <see cref="ILogger"/> API captures events, the SQL store persists them,
/// the background <c>DispatchWorker</c> polls and POSTs each payload to a stub HTTP endpoint,
/// and accepted deliveries are marked <see cref="DeliveryStatus.Delivered"/>.
///
/// The stub endpoint is a bare <see cref="HttpListener"/> bound to a free localhost port that
/// returns 200 and records every received request body — no ASP.NET hosting packages required.
///
/// If Docker is unavailable the test skips cleanly via <c>Skip.IfNot</c>.
/// </summary>
[Collection(SqlServerCollection.Name)]
public sealed class EndToEndTests
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

    private readonly SqlServerFixture _fixture;

    public EndToEndTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    [Trait("RequiresDocker", "true")]
    public async Task FullPath_Capture_Persist_Post_MarkDelivered()
    {
        Skip.IfNot(
            _fixture.IsAvailable,
            _fixture.SkipReason ?? "SQL Server test container is unavailable (Docker not detected).");

        var connectionString = _fixture.ConnectionString!;

        // 1-3 representative payloads emitted through the standard logging API. Each carries
        // globally-unique content so dedup keys never collide with rows from other tests.
        var token = Guid.NewGuid().ToString("N");
        var messages = new[]
        {
            $"e2e-info:{token}:order-created",
            $"e2e-warn:{token}:inventory-low",
            $"e2e-error:{token}:payment-declined"
        };

        // --- Stub HTTP endpoint: records each received POST body, always returns 200. ---
        using var stub = new StubLogBankEndpoint();
        stub.Start();

        // --- Compose the host using the library's real DI wiring (Req 1.2). ---
        using IHost host = new HostBuilder()
            .ConfigureServices(services =>
            {
                // Consumer supplies the EF Core context: a factory for the worker's reads/updates
                // and a scoped context for the store's transactional-write path.
                services.AddDbContextFactory<TestDbContext>(options => options.UseSqlServer(connectionString));
                services.AddScoped(sp =>
                {
                    var options = new DbContextOptionsBuilder<TestDbContext>()
                        .UseSqlServer(connectionString)
                        .Options;
                    return new TestDbContext(options);
                });

                services.AddLogsOutbox<TestDbContext>(options =>
                {
                    options.StoreType = OutboxStoreType.Sql;
                    options.LogBankEndpoint = stub.Url;
                    options.PollInterval = PollInterval;
                    options.RequestTimeout = TimeSpan.FromSeconds(10);
                    options.BatchSize = 100;
                    options.RetryLimit = 3;
                    options.ContainerKey = "e2e-container";
                    options.Source = "e2e-source";
                    // Este test valida la captura por ILogger de extremo a extremo (opt-in).
                    options.EnableLoggerProvider = true;
                });
            })
            .Build();

        // Sanity: the provider, store, and hosted worker are all resolvable (Req 1.2).
        var providers = host.Services.GetServices<ILoggerProvider>().ToList();
        Assert.Contains(providers, p => p is IATec.Shared.Net.OutboxLog.Logging.OutboxLoggerProvider);
        var hostedServices = host.Services.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, s => s is IATec.Shared.Net.OutboxLog.Dispatch.DispatchWorker);

        // Starting the host runs the table initializer and starts the DispatchWorker.
        await host.StartAsync();

        try
        {
            // --- Capture: emit the events through an ILogger from the resolved provider. ---
            using var factory = new LoggerFactory(providers);
            ILogger logger = factory.CreateLogger("IATec.Shared.Net.OutboxLog.IntegrationTests.EndToEnd");

            logger.LogInformation("{Message}", messages[0]);
            logger.LogWarning("{Message}", messages[1]);
            logger.LogError("{Message}", messages[2]);

            var expectedKeys = messages
                .Select(BuildExpectedDedupKey)
                .ToHashSet();

            // --- Wait (bounded) until every entry is Delivered in the DB. ---
            var delivered = await WaitUntilAsync(
                async () =>
                {
                    var statuses = await QueryStatusesAsync(connectionString, expectedKeys);
                    return statuses.Count == expectedKeys.Count
                        && statuses.Values.All(s => s == DeliveryStatus.Delivered);
                },
                WaitTimeout);

            Assert.True(
                delivered,
                "Expected all emitted log entries to reach Delivered status within the timeout.");

            // --- Assert the stub actually received the POSTs (capture->persist->POST path). ---
            var receivedContents = stub.ReceivedContents
                .Select(ExtractContentField)
                .Where(c => c is not null)
                .Select(c => c!)
                .ToHashSet();

            foreach (var message in messages)
            {
                Assert.Contains(message, receivedContents);
            }

            // --- Assert every entry is Delivered in the DB (Req 5.3, accepted -> delivered). ---
            var finalStatuses = await QueryStatusesAsync(connectionString, expectedKeys);
            Assert.Equal(expectedKeys.Count, finalStatuses.Count);
            Assert.All(finalStatuses.Values, status => Assert.Equal(DeliveryStatus.Delivered, status));
        }
        finally
        {
            await host.StopAsync();
        }
    }

    /// <summary>
    /// Recomputes the deduplication key the library would assign to the persisted entry for a given
    /// log message, matching the payload the <c>LogPayloadFactory</c> builds (container/source from
    /// options, owner/action/userId empty, content = the formatted message).
    /// </summary>
    private static string BuildExpectedDedupKey(string message)
    {
        var payload = new LogPayload
        {
            ContainerKey = "e2e-container",
            Source = "e2e-source",
            Owner = "",
            Action = "",
            UserId = "",
            Content = message
        };
        return DeduplicationKeyGenerator.Compute(payload);
    }

    /// <summary>
    /// Reads the current <see cref="DeliveryStatus"/> for the entries matching the expected dedup
    /// keys directly from the outbox table.
    /// </summary>
    private static async Task<Dictionary<string, DeliveryStatus>> QueryStatusesAsync(
        string connectionString,
        IReadOnlySet<string> expectedKeys)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(connectionString)
            .Options;

        await using var context = new TestDbContext(options);

        var rows = await context.Set<OutboxEntry>()
            .Where(e => expectedKeys.Contains(e.DeduplicationKey))
            .Select(e => new { e.DeduplicationKey, e.Status })
            .ToListAsync();

        return rows.ToDictionary(r => r.DeduplicationKey, r => r.Status);
    }

    /// <summary>Pulls the <c>content</c> value out of a received JSON payload body.</summary>
    private static string? ExtractContentField(string json)
    {
        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(json);
            if (document.RootElement.TryGetProperty("content", out var content))
            {
                return content.GetString();
            }
        }
        catch (System.Text.Json.JsonException)
        {
            // Ignore non-JSON bodies.
        }

        return null;
    }

    /// <summary>Polls <paramref name="condition"/> until it holds or the timeout elapses.</summary>
    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return await condition();
    }
}

/// <summary>
/// A minimal in-process HTTP endpoint backed by <see cref="HttpListener"/>. It binds to a free
/// localhost port, answers every request with 200 OK, and records each received request body so
/// the test can assert what the dispatch worker POSTed. No ASP.NET hosting dependency is required.
/// </summary>
internal sealed class StubLogBankEndpoint : IDisposable
{
    private readonly HttpListener _listener = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentQueue<string> _received = new();
    private Task? _loop;

    public StubLogBankEndpoint()
    {
        var port = GetFreeTcpPort();
        Url = $"http://127.0.0.1:{port}/v1/log";
        _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
    }

    /// <summary>Absolute URL the dispatch worker should POST to.</summary>
    public string Url { get; }

    /// <summary>Snapshot of every request body received so far.</summary>
    public IReadOnlyCollection<string> ReceivedContents => _received.ToArray();

    public void Start()
    {
        _listener.Start();
        _loop = Task.Run(AcceptLoopAsync);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync();
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (HttpListenerException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            try
            {
                using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                {
                    var body = await reader.ReadToEndAsync();
                    _received.Enqueue(body);
                }

                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.ContentLength64 = 0;
            }
            catch
            {
                try
                {
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                }
                catch
                {
                    // Best effort.
                }
            }
            finally
            {
                try
                {
                    context.Response.OutputStream.Close();
                }
                catch
                {
                    // Best effort.
                }
            }
        }
    }

    private static int GetFreeTcpPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    public void Dispose()
    {
        _cts.Cancel();

        try
        {
            if (_listener.IsListening)
            {
                _listener.Stop();
            }
        }
        catch
        {
            // Best effort.
        }

        _listener.Close();

        try
        {
            _loop?.Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Best effort; loop terminates on listener disposal.
        }

        _cts.Dispose();
    }
}
