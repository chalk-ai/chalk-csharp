using Chalk;
using Chalk.Exceptions;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

[TestFixture]
[Category("Integration")]
public class IntegrationTests
{
    // Set these environment variables to run integration tests:
    // CHALK_CLIENT_ID, CHALK_CLIENT_SECRET, CHALK_ACTIVE_ENVIRONMENT, CHALK_API_SERVER
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

    [Test]
    public async Task HttpClient_CanAuthenticate()
    {
        using var client = ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .Build();

        // If we get here without exception, authentication succeeded
        client.PrintConfig();
        Assert.Pass("Authentication successful");
    }

    [Test]
    public async Task GrpcClient_CanAuthenticate()
    {
        using var client = ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithGrpc()
            .Build();

        // If we get here without exception, authentication succeeded
        client.PrintConfig();

        // Check that we got the gRPC engine URL
        if (client is GrpcChalkClient grpcClient)
        {
            var engineUrl = grpcClient.GetGrpcEngineUrl();
            Console.WriteLine($"gRPC Engine URL: {engineUrl}");
        }

        Assert.Pass("gRPC Authentication successful");
    }

    [Test]
    public async Task HttpClient_CanQueryFeatures()
    {
        using var client = ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();

        // Query user.id=1 and request user.id as output
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.id")
            .WithIncludeMeta()
            .WithQueryName("csharp-integration-test")
            .Build();

        try
        {
            var result = await client.OnlineQueryAsync(queryParams);

            Console.WriteLine($"Query completed successfully");
            if (result.Meta != null)
            {
                Console.WriteLine($"  Query ID: {result.Meta.QueryId}");
                Console.WriteLine($"  Execution Duration: {result.Meta.ExecutionDurationS}s");
                Console.WriteLine($"  Environment: {result.Meta.EnvironmentName}");
            }

            if (result.Errors.Count > 0)
            {
                Console.WriteLine($"  Errors: {result.Errors.Count}");
                foreach (var error in result.Errors)
                {
                    Console.WriteLine($"    - {error}");
                }
            }

            Console.WriteLine($"  Data features: {result.Data.Count}");
            foreach (var (feature, values) in result.Data)
            {
                Console.WriteLine($"    - {feature}: {string.Join(", ", values)}");
            }

            // Verify we got the user.id back
            var userId = result.GetValue<object>("user.id");
            Console.WriteLine($"  user.id value: {userId}");

            Assert.Pass("Query completed");
        }
        catch (ServerException ex)
        {
            Console.WriteLine($"Server error: {ex.StatusCode} - {ex.Message}");
            Assert.Fail($"Server error: {ex.Message}");
        }
    }

    [Test]
    public async Task GrpcClient_CanQueryFeatures()
    {
        using var client = ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithGrpc()
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.id")
            .WithIncludeMeta()
            .WithQueryName("csharp-grpc-integration-test")
            .Build();

        try
        {
            var result = await client.OnlineQueryAsync(queryParams);

            Console.WriteLine($"gRPC Query completed successfully");
            if (result.Meta != null)
            {
                Console.WriteLine($"  Query ID: {result.Meta.QueryId}");
                Console.WriteLine($"  Execution Duration: {result.Meta.ExecutionDurationS}s");
            }

            var userId = result.GetValue<object>("user.id");
            Console.WriteLine($"  user.id value: {userId}");

            Assert.Pass("gRPC Query completed");
        }
        catch (ServerException ex)
        {
            Console.WriteLine($"Server error: {ex.StatusCode} - {ex.Message}");
            Assert.Fail($"Server error: {ex.Message}");
        }
    }

    [Test]
    public async Task HttpClient_BulkQuery()
    {
        using var client = ChalkClient.Builder()
            .WithClientId(ClientId!)
            .WithClientSecret(ClientSecret!)
            .WithEnvironmentId(EnvironmentId!)
            .WithApiServer(ApiServer!)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .Build();

        // Bulk query with multiple inputs
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.id")
            .WithIncludeMeta()
            .WithQueryName("csharp-bulk-integration-test")
            .Build();

        try
        {
            var result = await client.OnlineQueryAsync(queryParams);

            Console.WriteLine($"Bulk query completed");
            if (result.Meta != null)
            {
                Console.WriteLine($"  Query ID: {result.Meta.QueryId}");
            }

            var userIds = result.GetValues<object>("user.id");
            Console.WriteLine($"  Results count: {userIds.Count}");
            Console.WriteLine($"  user.id values: {string.Join(", ", userIds)}");

            Assert.Pass("Bulk query completed");
        }
        catch (ServerException ex)
        {
            Console.WriteLine($"Server error: {ex.StatusCode} - {ex.Message}");
            Assert.Fail($"Server error: {ex.Message}");
        }
    }
}
