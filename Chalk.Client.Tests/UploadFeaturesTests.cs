using System.Net;
using System.Text;
using Chalk;
using Chalk.Exceptions;
using Chalk.Internal;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

[TestFixture]
public class UploadFeaturesTests
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

    private static string UploadResponse() =>
        """
        {
            "operation_id": "op-upload-123",
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
    public async Task UploadFeatures_Success_ReturnsOperationId()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/upload_features/multi", HttpStatusCode.OK, UploadResponse());

        using var client = CreateClient(handler);

        var inputs = new Dictionary<string, IList<object?>>
        {
            ["user.id"] = new List<object?> { 1, 2, 3 },
            ["user.name"] = new List<object?> { "Alice", "Bob", "Charlie" }
        };

        var result = await client.UploadFeaturesAsync(inputs);

        Assert.That(result.OperationId, Is.EqualTo("op-upload-123"));
        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public async Task UploadFeatures_VerifiesRequestFormat()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/upload_features/multi", HttpStatusCode.OK, UploadResponse());

        using var client = CreateClient(handler);

        var inputs = new Dictionary<string, IList<object?>>
        {
            ["user.id"] = new List<object?> { 1, 2 },
            ["user.name"] = new List<object?> { "Alice", "Bob" }
        };

        await client.UploadFeaturesAsync(inputs);

        var uploadRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/upload_features/multi");
        Assert.That(uploadRequest.BodyBytes, Is.Not.Null);

        // Verify ByteBaseModel format: starts with magic bytes
        var magic = Encoding.ASCII.GetString(uploadRequest.BodyBytes!, 0, 23);
        Assert.That(magic, Is.EqualTo("CHALK_BYTE_TRANSMISSION"));

        // Verify we can round-trip parse the request body
        var (jsonAttrs, sections) = BinaryProtocol.ParseByteBaseModel(uploadRequest.BodyBytes!);
        Assert.That(jsonAttrs, Does.Contain("columns"));
        Assert.That(sections, Contains.Key("table_bytes"));
        Assert.That(sections["table_bytes"].Length, Is.GreaterThan(0));
    }

    [Test]
    public async Task UploadFeatures_HitsQueryEngine_NotApiServer()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK,
            TokenResponseWithEngines());
        handler.Enqueue(HttpMethod.Post, "/v1/upload_features/multi", HttpStatusCode.OK, UploadResponse());

        using var client = CreateClient(handler);

        var inputs = new Dictionary<string, IList<object?>>
        {
            ["user.id"] = new List<object?> { 1 }
        };

        await client.UploadFeaturesAsync(inputs);

        // Upload should go to query engine, not API server
        var uploadRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/upload_features/multi");
        Assert.That(uploadRequest.Uri.Host, Is.EqualTo("engine.mock.chalk.test"));
    }

    [Test]
    public void UploadFeatures_EmptyInputs_Throws()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());

        using var client = CreateClient(handler);

        var inputs = new Dictionary<string, IList<object?>>();

        var ex = Assert.ThrowsAsync<ClientException>(
            async () => await client.UploadFeaturesAsync(inputs));

        Assert.That(ex!.Message, Does.Contain("empty"));
    }

    [Test]
    public void UploadFeatures_Sync_ReturnsOperationId()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.Enqueue(HttpMethod.Post, "/v1/upload_features/multi", HttpStatusCode.OK, UploadResponse());

        using var client = CreateClient(handler);

        var inputs = new Dictionary<string, IList<object?>>
        {
            ["user.id"] = new List<object?> { 1 }
        };

        var result = client.UploadFeatures(inputs);

        Assert.That(result.OperationId, Is.EqualTo("op-upload-123"));
    }
}
