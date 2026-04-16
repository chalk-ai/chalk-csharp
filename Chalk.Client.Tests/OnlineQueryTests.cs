using System.Net;
using Chalk;
using Chalk.Exceptions;
using Chalk.Models;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Chalk.Client.Tests;

/// <summary>
/// Online query tests using a mock HTTP handler.
/// These tests verify the full client request/response flow without making real network calls.
/// </summary>
[TestFixture]
public class OnlineQueryTests
{
    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    private static string TokenResponse(string token = "test-jwt") =>
        $$"""
        {
            "access_token": "{{token}}",
            "token_type": "bearer",
            "expires_in": 3600,
            "primary_environment": "test-env"
        }
        """;

    private static string TokenResponseWithEngines(
        string token = "test-jwt",
        string env = "test-env",
        string engineUrl = "https://engine.mock.chalk.test") =>
        $$"""
        {
            "access_token": "{{token}}",
            "token_type": "bearer",
            "expires_in": 3600,
            "primary_environment": "{{env}}",
            "engines": {
                "{{env}}": "{{engineUrl}}"
            }
        }
        """;

    private static string GrpcTokenResponse(string token = "test-jwt", string env = "test-env") =>
        $$"""
        {
            "access_token": "{{token}}",
            "token_type": "bearer",
            "expires_in": 3600,
            "primary_environment": "{{env}}",
            "engines": {
                "{{env}}": "https://engine.mock.chalk.test"
            },
            "grpc_engines": {
                "{{env}}": "https://grpc-engine.mock.chalk.test"
            }
        }
        """;

    private static string QueryResponse() =>
        """
        {
            "data": [
                {"field": "user.name", "value": "John Doe", "pkey": null, "ts": null, "valid": true, "error": null},
                {"field": "user.age", "value": 25, "pkey": null, "ts": null, "valid": true, "error": null}
            ],
            "errors": [],
            "meta": {
                "execution_duration_s": 0.05,
                "query_id": "q-123",
                "deployment_id": "d-456",
                "environment_id": "test-env",
                "environment_name": "Test Environment",
                "query_hash": "hash-789",
                "query_timestamp": "2026-02-22T00:00:00Z"
            }
        }
        """;

    private static IChalkClient CreateClient(MockHttpHandler handler) =>
        ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithHttpClient(new HttpClient(handler))
            .Build();

    // ----------------------------------------------------------------
    // Happy path
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies successful online query with correct response parsing.
    /// </summary>
    [Test]
    public async Task OnlineQuery_Success_ReturnsFeatureValues()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name", "user.age")
            .WithIncludeMeta()
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));
        Assert.That(result.GetValue<long>("user.age"), Is.EqualTo(25));
        Assert.That(result.Errors, Is.Empty);
        Assert.That(result.Meta, Is.Not.Null);
        Assert.That(result.Meta!.QueryId, Is.EqualTo("q-123"));
        Assert.That(result.Meta.ExecutionDurationS, Is.EqualTo(0.05));
    }

    /// <summary>
    /// Verifies the synchronous OnlineQuery wrapper works correctly.
    /// </summary>
    [Test]
    public void OnlineQuery_Sync_ReturnsFeatureValues()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name", "user.age")
            .Build();

        var result = client.OnlineQuery(queryParams);

        Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));
        Assert.That(result.GetValue<long>("user.age"), Is.EqualTo(25));
    }

    /// <summary>
    /// Verifies query metadata is correctly parsed from the response.
    /// </summary>
    [Test]
    public async Task OnlineQuery_ParsesQueryMetadata()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithIncludeMeta()
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Assert.That(result.Meta, Is.Not.Null);
        Assert.That(result.Meta!.QueryId, Is.EqualTo("q-123"));
        Assert.That(result.Meta.DeploymentId, Is.EqualTo("d-456"));
        Assert.That(result.Meta.EnvironmentId, Is.EqualTo("test-env"));
        Assert.That(result.Meta.EnvironmentName, Is.EqualTo("Test Environment"));
        Assert.That(result.Meta.ExecutionDurationS, Is.EqualTo(0.05));
        Assert.That(result.Meta.QueryHash, Is.EqualTo("hash-789"));
    }

    // ----------------------------------------------------------------
    // Error handling
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies API error throws ServerException with structured error details.
    /// </summary>
    [Test]
    public void OnlineQuery_ApiError_ThrowsServerException()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.InternalServerError,
            """{"code": "INTERNAL_ERROR", "message": "Something went wrong"}""");

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        var ex = Assert.ThrowsAsync<ServerException>(
            async () => await client.OnlineQueryAsync(queryParams));

        Assert.That(ex!.StatusCode, Is.EqualTo(500));
        Assert.That(ex.ErrorCode, Is.EqualTo("INTERNAL_ERROR"));
        Assert.That(ex.ErrorMessage, Is.EqualTo("Something went wrong"));
    }

    /// <summary>
    /// Verifies 400 error with structured JSON parses code and message.
    /// </summary>
    [Test]
    public void OnlineQuery_ValidationError_ParsesErrorDetails()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.BadRequest,
            """{"code": "VALIDATION_ERROR", "message": "Invalid feature name: user.fake"}""");

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.fake")
            .Build();

        var ex = Assert.ThrowsAsync<ServerException>(
            async () => await client.OnlineQueryAsync(queryParams));

        Assert.That(ex!.StatusCode, Is.EqualTo(400));
        Assert.That(ex.ErrorCode, Is.EqualTo("VALIDATION_ERROR"));
        Assert.That(ex.ErrorMessage, Does.Contain("Invalid feature name"));
    }

    /// <summary>
    /// Verifies non-JSON error responses are still captured in the exception.
    /// </summary>
    [Test]
    public void OnlineQuery_NonJsonError_IncludesRawBody()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.BadGateway,
            "Bad Gateway: upstream timeout");

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        var ex = Assert.ThrowsAsync<ServerException>(
            async () => await client.OnlineQueryAsync(queryParams));

        Assert.That(ex!.StatusCode, Is.EqualTo(502));
        Assert.That(ex.Message, Does.Contain("Bad Gateway"));
    }

    /// <summary>
    /// Verifies server-side feature errors in a successful response are parsed.
    /// </summary>
    [Test]
    public async Task OnlineQuery_ParsesServerErrors()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK,
            """
            {
                "data": [
                    {"field": "user.name", "value": null, "error": "resolver failed"}
                ],
                "errors": [
                    {
                        "code": "RESOLVER_FAILED",
                        "category": "FIELD",
                        "message": "Failed to resolve feature",
                        "feature": "user.name",
                        "resolver": "get_user_name"
                    }
                ]
            }
            """);

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Assert.That(result.Errors, Has.Count.EqualTo(1));
        Assert.That(result.Errors[0].Code, Is.EqualTo("RESOLVER_FAILED"));
        Assert.That(result.Errors[0].Category, Is.EqualTo("FIELD"));
        Assert.That(result.Errors[0].Message, Is.EqualTo("Failed to resolve feature"));
        Assert.That(result.Errors[0].Feature, Is.EqualTo("user.name"));
        Assert.That(result.Errors[0].Resolver, Is.EqualTo("get_user_name"));
    }

    // ----------------------------------------------------------------
    // 401 retry
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies the client automatically retries with a fresh token on 401 Unauthorized.
    /// </summary>
    [Test]
    public async Task OnlineQuery_Unauthorized_RetriesWithFreshToken()
    {
        var handler = new MockHttpHandler();
        // First token exchange
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            TokenResponse("expired-jwt"));
        // Query returns 401
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.Unauthorized,
            """{"code": "UNAUTHORIZED", "message": "Token expired"}""");
        // Fresh token exchange
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            TokenResponse("fresh-jwt"));
        // Retry succeeds
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));

        // Verify: 2 token requests and 2 query requests
        var tokenRequests = handler.Requests
            .Where(r => r.Uri.AbsolutePath == "/v1/oauth/token").ToList();
        var queryRequests = handler.Requests
            .Where(r => r.Uri.AbsolutePath == "/v1/query/online").ToList();

        Assert.That(tokenRequests, Has.Count.EqualTo(2));
        Assert.That(queryRequests, Has.Count.EqualTo(2));

        // Verify the retry used the fresh token
        Assert.That(queryRequests[1].GetHeader("Authorization"), Is.EqualTo("Bearer fresh-jwt"));
    }

    // ----------------------------------------------------------------
    // Header verification
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies all expected headers are sent with the query request.
    /// </summary>
    [Test]
    public async Task OnlineQuery_VerifiesRequestHeaders()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithQueryName("test-query")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");

        Assert.That(queryRequest.GetHeader("Authorization"), Is.EqualTo("Bearer test-jwt"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Env-Id"), Is.EqualTo("test-env"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Client-Id"), Is.EqualTo("test-client-id"));
        Assert.That(queryRequest.GetHeader("User-Agent"), Is.EqualTo("chalk-csharp"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Deployment-Type"), Is.EqualTo("engine"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Query-Name"), Is.EqualTo("test-query"));
    }

    /// <summary>
    /// Verifies query_name and query_name_version are sent in the request body.
    /// </summary>
    [Test]
    public async Task OnlineQuery_VerifiesRequestBody()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithQueryName("test-query")
            .WithQueryNameVersion("v2")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        Assert.That(queryRequest.Body, Does.Contain("\"query_name\":\"test-query\""));
        Assert.That(queryRequest.Body, Does.Contain("\"query_name_version\":\"v2\""));
    }

    /// <summary>
    /// Verifies branch routing sets the correct deployment type and branch ID headers.
    /// </summary>
    [Test]
    public async Task OnlineQuery_WithBranch_SendsBranchDeploymentType()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithBranch("feature-branch")
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");

        Assert.That(queryRequest.GetHeader("X-Chalk-Branch-Id"), Is.EqualTo("feature-branch"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Deployment-Type"), Is.EqualTo("branch"));
    }

    /// <summary>
    /// Verifies deployment tag header is sent when configured.
    /// </summary>
    [Test]
    public async Task OnlineQuery_WithDeploymentTag_SendsTagHeader()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithDeploymentTag("v2.0")
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");

        Assert.That(queryRequest.GetHeader("X-Chalk-Deployment-Tag"), Is.EqualTo("v2.0"));
        // Without a branch, deployment type should be "engine"
        Assert.That(queryRequest.GetHeader("X-Chalk-Deployment-Type"), Is.EqualTo("engine"));
    }

    // ----------------------------------------------------------------
    // Request body verification
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies single-row query serializes input as a scalar (not array).
    /// </summary>
    [Test]
    public async Task OnlineQuery_SingleRow_SerializesInputAsScalar()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name", "user.age")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        var body = JObject.Parse(queryRequest.Body!);

        // Single-row input should be unwrapped from array to scalar
        Assert.That((int)body["inputs"]!["user.id"]!, Is.EqualTo(1));

        var outputs = body["outputs"]!.ToObject<List<string>>();
        Assert.That(outputs, Does.Contain("user.name"));
        Assert.That(outputs, Does.Contain("user.age"));
    }

    /// <summary>
    /// Verifies multi-row query keeps inputs as arrays.
    /// </summary>
    [Test]
    public async Task OnlineQuery_MultiRow_SerializesInputsAsArrays()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK,
            """
            {
                "data": [
                    {"field": "user.name", "value": "Alice"},
                    {"field": "user.name", "value": "Bob"},
                    {"field": "user.name", "value": "Charlie"}
                ],
                "errors": []
            }
            """);

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.name")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        // Verify request body has array inputs
        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        var body = JObject.Parse(queryRequest.Body!);

        var userIds = body["inputs"]!["user.id"]!.ToObject<List<int>>();
        Assert.That(userIds, Is.EquivalentTo(new[] { 1, 2, 3 }));

        // Verify response: "user.name" appears 3 times, grouped into one key
        var names = result.GetValues<string>("user.name");
        Assert.That(names, Has.Count.EqualTo(3));
    }

    /// <summary>
    /// Verifies all optional query fields are serialized correctly.
    /// </summary>
    [Test]
    public async Task OnlineQuery_WithAllOptions_SerializesAllFields()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithStaleness(new Dictionary<string, TimeSpan>
            {
                ["user.name"] = TimeSpan.FromMinutes(5)
            })
            .WithMeta(new Dictionary<string, string> { ["source"] = "test" })
            .WithTags("tag1", "tag2")
            .WithIncludeMeta()
            .WithExplain()
            .WithStorePlanStages()
            .WithCorrelationId("corr-123")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        var body = JObject.Parse(queryRequest.Body!);

        Assert.That((string)body["staleness"]!["user.name"]!, Is.EqualTo("300s"));
        Assert.That((string)body["meta"]!["source"]!, Is.EqualTo("test"));
        Assert.That(body["tags"]!.ToObject<List<string>>(), Is.EquivalentTo(new[] { "tag1", "tag2" }));
        Assert.That((bool)body["include_meta"]!, Is.True);
        Assert.That((bool)body["explain"]!, Is.True);
        Assert.That((bool)body["store_plan_stages"]!, Is.True);
        Assert.That((string)body["correlation_id"]!, Is.EqualTo("corr-123"));
    }

    // ----------------------------------------------------------------
    // URL routing
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies query server override routes queries to the override URL.
    /// </summary>
    [Test]
    public async Task OnlineQuery_QueryServerOverride_RoutesToOverride()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithQueryServer("https://query.mock.chalk.test")
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        // Token request goes to API server
        var tokenRequest = handler.Requests.First(r => r.Uri.AbsolutePath == "/v1/oauth/token");
        Assert.That(tokenRequest.Uri.Host, Is.EqualTo("api.mock.chalk.test"));

        // Query request goes to query server override
        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        Assert.That(queryRequest.Uri.Host, Is.EqualTo("query.mock.chalk.test"));
    }

    /// <summary>
    /// Verifies engine URL from token response is used for queries.
    /// </summary>
    [Test]
    public async Task OnlineQuery_EngineFromToken_RoutesToEngine()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            TokenResponseWithEngines());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        Assert.That(queryRequest.Uri.Host, Is.EqualTo("engine.mock.chalk.test"));
    }

    /// <summary>
    /// Verifies fallback to API server when no engine matches the environment.
    /// </summary>
    [Test]
    public async Task OnlineQuery_NoEngineMatch_FallsBackToApiServer()
    {
        var handler = new MockHttpHandler();
        // Token response has engine for a different environment
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            TokenResponseWithEngines(env: "other-env"));
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        // Query should fall back to API server since "test-env" has no engine
        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        Assert.That(queryRequest.Uri.Host, Is.EqualTo("api.mock.chalk.test"));
    }

    // ----------------------------------------------------------------
    // Token management
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies token is cached across multiple queries (only one token exchange).
    /// </summary>
    [Test]
    public async Task TokenExchange_CachesToken_AcrossMultipleQueries()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        // Two consecutive queries
        await client.OnlineQueryAsync(queryParams);
        await client.OnlineQueryAsync(queryParams);

        // Only one token exchange should have been made
        var tokenRequests = handler.Requests
            .Where(r => r.Uri.AbsolutePath == "/v1/oauth/token").ToList();
        Assert.That(tokenRequests, Has.Count.EqualTo(1));
    }

    /// <summary>
    /// Verifies token exchange failure throws ClientException.
    /// </summary>
    [Test]
    public void TokenExchange_Failure_ThrowsClientException()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.Unauthorized,
            """{"error": "invalid_client", "error_description": "Invalid credentials"}""");

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        var ex = Assert.ThrowsAsync<ClientException>(
            async () => await client.OnlineQueryAsync(queryParams));

        Assert.That(ex!.Message, Does.Contain("Failed to get access token"));
        Assert.That(ex.Message, Does.Contain("Unauthorized"));
    }

    /// <summary>
    /// Verifies token exchange sends correct client credentials.
    /// </summary>
    [Test]
    public async Task TokenExchange_SendsClientCredentials()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var tokenRequest = handler.Requests.First(r => r.Uri.AbsolutePath == "/v1/oauth/token");

        // Verify token request body
        var body = JObject.Parse(tokenRequest.Body!);
        Assert.That((string)body["client_id"]!, Is.EqualTo("test-client-id"));
        Assert.That((string)body["client_secret"]!, Is.EqualTo("test-client-secret"));
        Assert.That((string)body["grant_type"]!, Is.EqualTo("client_credentials"));

        // Verify token request headers
        Assert.That(tokenRequest.GetHeader("User-Agent"), Is.EqualTo("chalk-csharp"));
        Assert.That(tokenRequest.Uri.AbsolutePath, Is.EqualTo("/v1/oauth/token"));
        Assert.That(tokenRequest.Uri.Host, Is.EqualTo("api.mock.chalk.test"));
    }

    // ----------------------------------------------------------------
    // gRPC client
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies the gRPC client performs online queries correctly.
    /// The gRPC client currently uses HTTP for queries but sends a different User-Agent.
    /// </summary>
    [Test]
    public async Task GrpcClient_OnlineQuery_Success()
    {
        var handler = new MockHttpHandler();
        // GrpcChalkClient eagerly fetches a token during construction
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            GrpcTokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithGrpc()
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name", "user.age")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));
        Assert.That(result.GetValue<long>("user.age"), Is.EqualTo(25));

        // Verify gRPC-specific User-Agent
        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/online");
        Assert.That(queryRequest.GetHeader("User-Agent"), Is.EqualTo("chalk-csharp-grpc"));
    }

    /// <summary>
    /// Verifies the gRPC client eagerly exchanges a token during construction,
    /// and caches it for subsequent queries.
    /// </summary>
    [Test]
    public async Task GrpcClient_EagerTokenExchange_OnConstruction()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            GrpcTokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/query/online", HttpStatusCode.OK, QueryResponse());

        // Token exchange happens here (during Build)
        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithGrpc()
            .WithHttpClient(new HttpClient(handler))
            .Build();

        // Token request already made during construction
        var tokenRequestsBeforeQuery = handler.Requests
            .Where(r => r.Uri.AbsolutePath == "/v1/oauth/token").ToList();
        Assert.That(tokenRequestsBeforeQuery, Has.Count.EqualTo(1));

        // Query uses cached token — no additional token request
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .Build();

        await client.OnlineQueryAsync(queryParams);

        var tokenRequestsAfterQuery = handler.Requests
            .Where(r => r.Uri.AbsolutePath == "/v1/oauth/token").ToList();
        Assert.That(tokenRequestsAfterQuery, Has.Count.EqualTo(1));
    }
}
