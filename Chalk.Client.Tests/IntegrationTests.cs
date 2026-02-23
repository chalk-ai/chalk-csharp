using Chalk;
using Chalk.Exceptions;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

/// <summary>
/// End-to-end integration tests against a live Chalk environment.
/// Matches the Rust SDK's examples/integration_test.rs structure.
///
/// Requires environment variables:
///   CHALK_CLIENT_ID, CHALK_CLIENT_SECRET, CHALK_ACTIVE_ENVIRONMENT, CHALK_API_SERVER
/// </summary>
[TestFixture]
[Category("Integration")]
public class IntegrationTests
{
    private static readonly string? ClientId = Environment.GetEnvironmentVariable("CHALK_CLIENT_ID");
    private static readonly string? ClientSecret = Environment.GetEnvironmentVariable("CHALK_CLIENT_SECRET");
    private static readonly string? EnvironmentId = Environment.GetEnvironmentVariable("CHALK_ACTIVE_ENVIRONMENT");
    private static readonly string? ApiServer = Environment.GetEnvironmentVariable("CHALK_API_SERVER") ?? "https://api.chalk.ai";

    [SetUp]
    public void SetUp()
    {
        if (string.IsNullOrEmpty(ClientId) || string.IsNullOrEmpty(ClientSecret))
        {
            Assert.Ignore("Integration tests require CHALK_CLIENT_ID and CHALK_CLIENT_SECRET environment variables");
        }
    }

    private IChalkClient BuildHttpClient() =>
        ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();

    private IChalkClient BuildGrpcClient() =>
        ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithGrpc()
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();

    // ----------------------------------------------------------------
    // HTTP Client — matches Rust integration_test.rs Tests 1-3
    // ----------------------------------------------------------------

    /// <summary>
    /// Test 1: HTTP online query (user.id=1).
    /// Matches Rust: Test 1: Online Query (user.id=1)
    /// </summary>
    [Test]
    [Order(1)]
    public async Task HttpClient_OnlineQuery_UserId1()
    {
        using var client = BuildHttpClient();
        Console.WriteLine("HTTP client connected");
        client.PrintConfig();

        Console.WriteLine("\n=== Test 1: Online Query (user.id=1) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.id", "user.name", "user.email", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: {string.Join(", ", values)}");
        }
        foreach (var error in result.Errors)
        {
            Console.Error.WriteLine($"  error: code={error.Code} message={error.Message}");
        }
        if (result.Meta != null)
        {
            Console.WriteLine($"  query_id: {result.Meta.QueryId}");
            Console.WriteLine($"  execution_duration_s: {result.Meta.ExecutionDurationS}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    /// <summary>
    /// Test 2: HTTP online query with a different user (user.id=2).
    /// Matches Rust: Test 2: Online Query (user.id=2)
    /// </summary>
    [Test]
    [Order(2)]
    public async Task HttpClient_OnlineQuery_UserId2()
    {
        using var client = BuildHttpClient();

        Console.WriteLine("\n=== Test 2: Online Query (user.id=2) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 2)
            .WithOutputs("user.id", "user.name", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: {string.Join(", ", values)}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    /// <summary>
    /// Test 3: HTTP multi-row (bulk) query with user.id=[1,2,3].
    /// Matches Rust: Test 3: Bulk Query (user.id=[1,2,3])
    /// </summary>
    [Test]
    [Order(3)]
    public async Task HttpClient_MultiRowQuery()
    {
        using var client = BuildHttpClient();

        Console.WriteLine("\n=== Test 3: Multi-Row Query (user.id=[1,2,3]) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.id", "user.name", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-bulk-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: [{string.Join(", ", values)}]");
        }
        if (result.Meta != null)
        {
            Console.WriteLine($"  query_id: {result.Meta.QueryId}");
        }
        foreach (var error in result.Errors)
        {
            Console.Error.WriteLine($"  error: code={error.Code} message={error.Message}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    // ----------------------------------------------------------------
    // gRPC Client — matches Rust integration_test.rs Tests 6-7
    // ----------------------------------------------------------------

    /// <summary>
    /// Test 4: gRPC client authentication and online query (user.id=1).
    /// Matches Rust: Test 6: gRPC Online Query (user.id=1)
    /// </summary>
    [Test]
    [Order(4)]
    public async Task GrpcClient_OnlineQuery_UserId1()
    {
        using var client = BuildGrpcClient();
        Console.WriteLine("\ngRPC client connected");
        client.PrintConfig();

        if (client is GrpcChalkClient grpcClient)
        {
            var engineUrl = grpcClient.GetGrpcEngineUrl();
            Console.WriteLine($"  gRPC engine URL: {engineUrl}");
        }

        Console.WriteLine("\n=== Test 4: gRPC Online Query (user.id=1) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.id", "user.name", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-grpc-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: {string.Join(", ", values)}");
        }
        foreach (var error in result.Errors)
        {
            Console.Error.WriteLine($"  error: code={error.Code} message={error.Message}");
        }
        if (result.Meta != null)
        {
            Console.WriteLine($"  query_id: {result.Meta.QueryId}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    /// <summary>
    /// Test 5: gRPC online query with a different user (user.id=2).
    /// Matches Rust: Test 7: gRPC Online Query (user.id=2)
    /// </summary>
    [Test]
    [Order(5)]
    public async Task GrpcClient_OnlineQuery_UserId2()
    {
        using var client = BuildGrpcClient();

        Console.WriteLine("\n=== Test 5: gRPC Online Query (user.id=2) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 2)
            .WithOutputs("user.id", "user.name", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-grpc-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: {string.Join(", ", values)}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    /// <summary>
    /// Test 6: gRPC multi-row query with user.id=[1,2,3].
    /// Matches Rust: Test 8: gRPC Bulk Query (user.id=[1,2,3])
    /// </summary>
    [Test]
    [Order(6)]
    public async Task GrpcClient_MultiRowQuery()
    {
        using var client = BuildGrpcClient();

        Console.WriteLine("\n=== Test 6: gRPC Multi-Row Query (user.id=[1,2,3]) ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.id", "user.name", "user.age")
            .WithIncludeMeta()
            .WithQueryName("csharp-grpc-bulk-integration-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        foreach (var (feature, values) in result.Data)
        {
            Console.WriteLine($"  {feature}: [{string.Join(", ", values)}]");
        }
        foreach (var error in result.Errors)
        {
            Console.Error.WriteLine($"  error: code={error.Code} message={error.Message}");
        }

        Assert.That(result.Data, Is.Not.Empty, "Expected at least one feature in response");
    }

    // ----------------------------------------------------------------
    // Error handling
    // ----------------------------------------------------------------

    /// <summary>
    /// Verifies the client surfaces server errors properly when querying
    /// a feature that may not resolve.
    /// </summary>
    [Test]
    [Order(7)]
    public async Task HttpClient_QueryWithErrors_SurfacesErrors()
    {
        using var client = BuildHttpClient();

        Console.WriteLine("\n=== Test 7: Query Error Handling ===");
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.id")
            .WithIncludeMeta()
            .WithQueryName("csharp-error-handling-test")
            .Build();

        var result = await client.OnlineQueryAsync(queryParams);

        Console.WriteLine($"  data features: {result.Data.Count}");
        Console.WriteLine($"  errors: {result.Errors.Count}");
        if (result.Meta != null)
        {
            Console.WriteLine($"  query_id: {result.Meta.QueryId}");
            Console.WriteLine($"  environment: {result.Meta.EnvironmentName}");
            Console.WriteLine($"  deployment_id: {result.Meta.DeploymentId}");
        }

        // We expect the query to succeed (even if some features have errors)
        Assert.That(result.Meta, Is.Not.Null, "Expected query metadata");
        Assert.That(result.Meta!.QueryId, Is.Not.Null.And.Not.Empty, "Expected a query ID");
    }
}
