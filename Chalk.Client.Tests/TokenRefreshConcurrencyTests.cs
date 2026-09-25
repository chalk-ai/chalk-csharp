using System.Net;
using System.Text;
using Chalk;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

/// <summary>
/// Verifies that concurrent callers sharing one client coalesce token refreshes
/// instead of each exchanging credentials on their own.
/// </summary>
[TestFixture]
public class TokenRefreshConcurrencyTests
{
    private const int Callers = 32;

    /// <summary>
    /// Thread-safe fake API server. Issues tokens "jwt-1", "jwt-2", ... and accepts a
    /// query only when it carries the newest token at or above <c>minValidToken</c>.
    /// Both endpoints delay so that concurrent callers overlap.
    /// </summary>
    private sealed class TokenServer : HttpMessageHandler
    {
        private readonly int _minValidToken;
        private int _issued;
        private int _unauthorized;

        public TokenServer(int minValidToken)
        {
            _minValidToken = minValidToken;
        }

        public int TokenRequests => Volatile.Read(ref _issued);
        public int UnauthorizedResponses => Volatile.Read(ref _unauthorized);

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            await Task.Delay(50, cancellationToken);

            if (request.RequestUri!.AbsolutePath == "/v1/oauth/token")
            {
                var n = Interlocked.Increment(ref _issued);
                return Json(HttpStatusCode.OK, $$"""
                    {"access_token": "jwt-{{n}}", "token_type": "bearer", "expires_in": 3600,
                     "primary_environment": "test-env"}
                    """);
            }

            var bearer = request.Headers.Authorization?.Parameter;
            var latest = Volatile.Read(ref _issued);
            if (latest < _minValidToken || bearer != $"jwt-{latest}")
            {
                Interlocked.Increment(ref _unauthorized);
                return Json(HttpStatusCode.Unauthorized, """{"code": "UNAUTHORIZED", "message": "stale"}""");
            }

            return Json(HttpStatusCode.OK, """
                {"data": [{"field": "user.name", "value": "John Doe", "pkey": null, "ts": null,
                           "valid": true, "error": null}],
                 "errors": []}
                """);
        }

        private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
            new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }

    private static ChalkClientBuilder Builder(TokenServer server) =>
        ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithHttpClient(new HttpClient(server));

    private static async Task RunConcurrentQueries(IChalkClient client)
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        using var start = new ManualResetEventSlim(false);
        var tasks = Enumerable.Range(0, Callers)
            .Select(_ => Task.Run(async () =>
            {
                start.Wait();
                return await client.OnlineQueryAsync(queryParams);
            }))
            .ToList();
        start.Set();

        var results = await Task.WhenAll(tasks);
        foreach (var result in results)
        {
            Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));
        }
    }

    [Test]
    public async Task ConcurrentFirstQueries_ExchangeTokenOnce()
    {
        var server = new TokenServer(minValidToken: 1);
        using var client = Builder(server).Build();

        await RunConcurrentQueries(client);

        Assert.That(server.TokenRequests, Is.EqualTo(1));
    }

    [Test]
    public async Task ConcurrentUnauthorizedResponses_RefreshTokenOnce()
    {
        // The first token is rejected, so every caller sees a 401 and forces a refresh.
        var server = new TokenServer(minValidToken: 2);
        using var client = Builder(server).Build();

        await RunConcurrentQueries(client);

        Assert.That(server.UnauthorizedResponses, Is.EqualTo(Callers));
        Assert.That(server.TokenRequests, Is.EqualTo(2));
    }

    [Test]
    public async Task Grpc_ConcurrentUnauthorizedResponses_RefreshTokenOnce()
    {
        // The constructor exchanges jwt-1, which the server rejects.
        var server = new TokenServer(minValidToken: 2);
        using var client = Builder(server).WithGrpc().Build();

        await RunConcurrentQueries(client);

        Assert.That(server.UnauthorizedResponses, Is.EqualTo(Callers));
        Assert.That(server.TokenRequests, Is.EqualTo(2));
    }
}
