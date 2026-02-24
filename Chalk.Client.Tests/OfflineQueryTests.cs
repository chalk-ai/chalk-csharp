using System.Net;
using Chalk;
using Chalk.Exceptions;
using Chalk.Models;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Chalk.Client.Tests;

[TestFixture]
public class OfflineQueryTests
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

    private static string OfflineQueryResponse() =>
        """
        {
            "is_finished": false,
            "version": 4,
            "dataset_id": "ds-123",
            "dataset_name": "my-dataset",
            "environment_id": "test-env",
            "dataset_revision_id": "rev-456",
            "revisions": [
                {
                    "revision_id": "rev-456",
                    "creator_id": "user-1",
                    "environment_id": "test-env",
                    "outputs": ["user.name", "user.age"],
                    "status": "pending",
                    "num_partitions": 1,
                    "dashboard_url": "https://chalk.ai/datasets/ds-123",
                    "dataset_name": "my-dataset",
                    "dataset_id": "ds-123",
                    "branch": null,
                    "created_at": "2026-02-22T00:00:00Z",
                    "started_at": null,
                    "terminated_at": null
                }
            ],
            "errors": []
        }
        """;

    private static string StatusResponse(string status) =>
        $$"""
        {
            "report": {
                "status": "{{status}}",
                "operation_id": "op-789",
                "environment_id": "test-env",
                "error": null
            }
        }
        """;

    private static string DownloadUrlsResponse(bool isFinished = true) =>
        $$"""
        {
            "is_finished": {{(isFinished ? "true" : "false")}},
            "urls": ["https://storage.example.com/file1.parquet", "https://storage.example.com/file2.parquet"],
            "errors": []
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

    [Test]
    public async Task OfflineQuery_Success_ReturnsDataset()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.name", "user.age")
            .Build();

        var result = await client.OfflineQueryAsync(queryParams);

        Assert.That(result.DatasetId, Is.EqualTo("ds-123"));
        Assert.That(result.DatasetName, Is.EqualTo("my-dataset"));
        Assert.That(result.EnvironmentId, Is.EqualTo("test-env"));
        Assert.That(result.DatasetRevisionId, Is.EqualTo("rev-456"));
        Assert.That(result.Revisions, Has.Count.EqualTo(1));
        Assert.That(result.Revisions![0].RevisionId, Is.EqualTo("rev-456"));
        Assert.That(result.Revisions[0].Status, Is.EqualTo("pending"));
        Assert.That(result.Revisions[0].Outputs, Contains.Item("user.name"));
    }

    [Test]
    public async Task OfflineQuery_SerializesInputsCorrectly()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithInput("user.type", new List<object?> { "admin", "user", "user" })
            .WithOutputs("user.name")
            .Build();

        await client.OfflineQueryAsync(queryParams);

        var request = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v4/offline_query");
        var body = JObject.Parse(request.Body!);

        // Verify columnar input format
        var columns = body["input"]!["columns"]!.ToObject<List<string>>();
        Assert.That(columns, Contains.Item("user.id"));
        Assert.That(columns, Contains.Item("user.type"));

        var values = body["input"]!["values"]!;
        Assert.That(values.Count(), Is.EqualTo(3)); // 3 rows

        var outputs = body["output"]!.ToObject<List<string>>();
        Assert.That(outputs, Contains.Item("user.name"));
    }

    [Test]
    public async Task OfflineQuery_WithAllOptions_SerializesAllFields()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());

        using var client = CreateClient(handler);

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .WithDatasetName("test-dataset")
            .WithBranch("feature-branch")
            .WithMaxSamples(100)
            .WithTags("tag1", "tag2")
            .WithCorrelationId("corr-123")
            .WithStoreOnline(true)
            .WithStoreOffline(false)
            .WithRunAsynchronously()
            .WithExplain()
            .WithStorePlanStages()
            .WithMaxRetries(3)
            .Build();

        await client.OfflineQueryAsync(queryParams);

        var request = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v4/offline_query");
        var body = JObject.Parse(request.Body!);

        Assert.That((string)body["dataset_name"]!, Is.EqualTo("test-dataset"));
        Assert.That((string)body["branch"]!, Is.EqualTo("feature-branch"));
        Assert.That((int)body["max_samples"]!, Is.EqualTo(100));
        Assert.That(body["tags"]!.ToObject<List<string>>(), Is.EquivalentTo(new[] { "tag1", "tag2" }));
        Assert.That((string)body["correlation_id"]!, Is.EqualTo("corr-123"));
        Assert.That((bool)body["store_online"]!, Is.True);
        Assert.That((bool)body["store_offline"]!, Is.False);
        Assert.That((bool)body["run_asynchronously"]!, Is.True);
        Assert.That((bool)body["explain"]!, Is.True);
        Assert.That((bool)body["store_plan_stages"]!, Is.True);
        Assert.That((int)body["max_retries"]!, Is.EqualTo(3));
    }

    [Test]
    public async Task OfflineQuery_HitsApiServer_NotQueryEngine()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());

        using var client = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://api.mock.chalk.test")
            .WithQueryServer("https://engine.mock.chalk.test")
            .WithEnvironmentId("test-env")
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .Build();

        await client.OfflineQueryAsync(queryParams);

        // Offline query must go to API server, not query engine
        var offlineRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v4/offline_query");
        Assert.That(offlineRequest.Uri.Host, Is.EqualTo("api.mock.chalk.test"));
    }

    [Test]
    public void OfflineQuery_BuilderValidation_RequiresOutputs()
    {
        var builder = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 });

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    // ----------------------------------------------------------------
    // Status polling
    // ----------------------------------------------------------------

    [Test]
    public async Task WaitForOfflineQuery_PollsUntilComplete()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());
        // Status polls: pending, pending, succeeded
        handler.Enqueue(HttpMethod.Get, "/v4/offline_query/rev-456/status", HttpStatusCode.OK, StatusResponse("pending"));
        handler.Enqueue(HttpMethod.Get, "/v4/offline_query/rev-456/status", HttpStatusCode.OK, StatusResponse("pending"));
        handler.Enqueue(HttpMethod.Get, "/v4/offline_query/rev-456/status", HttpStatusCode.OK, StatusResponse("succeeded"));

        using var client = CreateClient(handler);

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .Build();

        var offlineResult = await client.OfflineQueryAsync(queryParams);
        var result = await client.WaitForOfflineQueryAsync(offlineResult, timeout: TimeSpan.FromMinutes(1));

        Assert.That(result, Is.SameAs(offlineResult));

        // Verify 3 status polls were made
        var statusRequests = handler.Requests
            .Where(r => r.Uri.AbsolutePath.Contains("/status")).ToList();
        Assert.That(statusRequests, Has.Count.EqualTo(3));
    }

    // ----------------------------------------------------------------
    // Download URLs
    // ----------------------------------------------------------------

    [Test]
    public async Task GetDownloadUrls_ReturnsUrls()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v4/offline_query", HttpStatusCode.OK, OfflineQueryResponse());
        handler.Enqueue(HttpMethod.Get, "/v2/offline_query/rev-456", HttpStatusCode.OK, DownloadUrlsResponse());

        using var client = CreateClient(handler);

        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .Build();

        var offlineResult = await client.OfflineQueryAsync(queryParams);
        var urls = await client.GetOfflineQueryDownloadUrlsAsync(offlineResult, timeout: TimeSpan.FromMinutes(1));

        Assert.That(urls, Has.Count.EqualTo(2));
        Assert.That(urls[0], Does.Contain("file1.parquet"));
        Assert.That(urls[1], Does.Contain("file2.parquet"));
    }
}

[TestFixture]
public class OfflineQueryParamsTests
{
    [Test]
    public void Builder_WithInputAndOutput_BuildsParams()
    {
        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.name", "user.email")
            .Build();

        Assert.That(queryParams.Input, Is.Not.Null);
        Assert.That(queryParams.Input!.Columns, Has.Count.EqualTo(1));
        Assert.That(queryParams.Input.Columns, Contains.Item("user.id"));
        Assert.That(queryParams.Input.Values, Has.Count.EqualTo(3)); // 3 rows
        Assert.That(queryParams.Outputs, Has.Count.EqualTo(2));
    }

    [Test]
    public void Builder_MultipleInputColumns_ConvertsToRowFormat()
    {
        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2 })
            .WithInput("user.type", new List<object?> { "admin", "user" })
            .WithOutputs("user.name")
            .Build();

        Assert.That(queryParams.Input, Is.Not.Null);
        Assert.That(queryParams.Input!.Columns, Has.Count.EqualTo(2));
        Assert.That(queryParams.Input.Values, Has.Count.EqualTo(2)); // 2 rows
        // Row 0: [1, "admin"], Row 1: [2, "user"]
        Assert.That(queryParams.Input.Values[0], Has.Count.EqualTo(2));
    }

    [Test]
    public void Builder_NoOutputs_ThrowsException()
    {
        var builder = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 });

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Test]
    public void Builder_NoInputs_BuildsWithoutInput()
    {
        var queryParams = new OfflineQueryParamsBuilder()
            .WithOutputs("user.name")
            .Build();

        Assert.That(queryParams.Input, Is.Null);
    }

    [Test]
    public void Builder_WithAllOptions_SetsAllFields()
    {
        var deadline = DateTimeOffset.UtcNow.AddHours(1);
        var queryParams = new OfflineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .WithDatasetName("ds")
            .WithBranch("branch")
            .WithMaxSamples(50)
            .WithObservedAtLowerBound(DateTimeOffset.UnixEpoch)
            .WithObservedAtUpperBound(DateTimeOffset.UtcNow)
            .WithTags("t1")
            .WithRequiredResolverTags("r1")
            .WithCorrelationId("cid")
            .WithStoreOnline()
            .WithStoreOffline()
            .WithRunAsynchronously()
            .WithMaxCacheAgeSecs(300)
            .WithRecomputeFeatures()
            .WithQueryName("qn")
            .WithQueryNameVersion("v1")
            .WithStorePlanStages()
            .WithExplain()
            .WithNumShards(4)
            .WithNumWorkers(8)
            .WithCompletionDeadline(deadline)
            .WithMaxRetries(2)
            .Build();

        Assert.That(queryParams.DatasetName, Is.EqualTo("ds"));
        Assert.That(queryParams.Branch, Is.EqualTo("branch"));
        Assert.That(queryParams.MaxSamples, Is.EqualTo(50));
        Assert.That(queryParams.Tags, Contains.Item("t1"));
        Assert.That(queryParams.RequiredResolverTags, Contains.Item("r1"));
        Assert.That(queryParams.CorrelationId, Is.EqualTo("cid"));
        Assert.That(queryParams.StoreOnline, Is.True);
        Assert.That(queryParams.StoreOffline, Is.True);
        Assert.That(queryParams.RunAsynchronously, Is.True);
        Assert.That(queryParams.MaxCacheAgeSecs, Is.EqualTo(300));
        Assert.That(queryParams.RecomputeFeatures, Is.True);
        Assert.That(queryParams.QueryName, Is.EqualTo("qn"));
        Assert.That(queryParams.QueryNameVersion, Is.EqualTo("v1"));
        Assert.That(queryParams.StorePlanStages, Is.True);
        Assert.That(queryParams.Explain, Is.True);
        Assert.That(queryParams.NumShards, Is.EqualTo(4));
        Assert.That(queryParams.NumWorkers, Is.EqualTo(8));
        Assert.That(queryParams.CompletionDeadline, Is.EqualTo(deadline));
        Assert.That(queryParams.MaxRetries, Is.EqualTo(2));
    }
}
