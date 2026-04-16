using System.Net;
using System.Text;
using Chalk;
using Chalk.Internal;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

[TestFixture]
public class BulkQueryTests
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

    private static byte[] BuildMockBulkResponse(byte[]? tableBytes = null)
    {
        tableBytes ??= Encoding.UTF8.GetBytes("mock-feather-data");
        var attrs = """{"outputs": ["user.name"]}""";
        return BinaryProtocol.BuildByteBaseModel(attrs, new[] { ("table_bytes", tableBytes) });
    }

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
    public async Task BulkQuery_Success_ReturnsResult()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.EnqueueBinary(HttpMethod.Post, "/v1/query/feather", HttpStatusCode.OK, BuildMockBulkResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.name")
            .Build();

        var result = await client.OnlineQueryBulkAsync(queryParams);

        Assert.That(result.HasData, Is.True);
        Assert.That(result.ScalarData, Is.Not.Null);
        Assert.That(result.ScalarData!.Length, Is.GreaterThan(0));
        Assert.That(result.Errors, Is.Empty);
        Assert.That(result.Meta, Does.Contain("outputs"));
    }

    [Test]
    public async Task BulkQuery_VerifiesRequestFormat()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.EnqueueBinary(HttpMethod.Post, "/v1/query/feather", HttpStatusCode.OK, BuildMockBulkResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2 })
            .WithOutputs("user.name", "user.age")
            .WithQueryName("bulk-test")
            .WithQueryNameVersion("v2")
            .Build();

        await client.OnlineQueryBulkAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/feather");
        Assert.That(queryRequest.BodyBytes, Is.Not.Null);

        // Verify "chal1" magic
        var magic = Encoding.ASCII.GetString(queryRequest.BodyBytes!, 0, 5);
        Assert.That(magic, Is.EqualTo("chal1"));

        // Verify query_name and query_name_version are in the feather header
        var b = queryRequest.BodyBytes!;
        long headerLen = 0;
        for (var i = 5; i < 13; i++) headerLen = (headerLen << 8) | b[i];
        var featherHeader = Encoding.UTF8.GetString(b, 13, (int)headerLen);
        Assert.That(featherHeader, Does.Contain("\"query_name\":\"bulk-test\""));
        Assert.That(featherHeader, Does.Contain("\"query_name_version\":\"v2\""));
    }

    [Test]
    public async Task BulkQuery_ParsesResponseCorrectly()
    {
        // Build a proper ByteBaseModel response with known data
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        var attrs = """{"outputs": ["user.name"], "query_id": "q-bulk-123"}""";
        var responseBytes = BinaryProtocol.BuildByteBaseModel(attrs, new[] { ("table_bytes", testData) });

        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.EnqueueBinary(HttpMethod.Post, "/v1/query/feather", HttpStatusCode.OK, responseBytes);

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .Build();

        var result = await client.OnlineQueryBulkAsync(queryParams);

        Assert.That(result.HasData, Is.True);
        Assert.That(result.ScalarData, Is.EqualTo(testData));
        Assert.That(result.Meta, Does.Contain("q-bulk-123"));
        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public void BulkQuery_Sync_ReturnsResult()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.EnqueueBinary(HttpMethod.Post, "/v1/query/feather", HttpStatusCode.OK, BuildMockBulkResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .Build();

        var result = client.OnlineQueryBulk(queryParams);

        Assert.That(result.HasData, Is.True);
    }

    [Test]
    public async Task BulkQuery_SendsCorrectHeaders()
    {
        var handler = new MockHttpHandler();
        handler.Enqueue(HttpMethod.Post, "/v1/oauth/token", HttpStatusCode.OK, TokenResponse());
        handler.EnqueueBinary(HttpMethod.Post, "/v1/query/feather", HttpStatusCode.OK, BuildMockBulkResponse());

        using var client = CreateClient(handler);

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1 })
            .WithOutputs("user.name")
            .WithQueryName("bulk-test")
            .Build();

        await client.OnlineQueryBulkAsync(queryParams);

        var queryRequest = handler.Requests.Last(r => r.Uri.AbsolutePath == "/v1/query/feather");
        Assert.That(queryRequest.GetHeader("Authorization"), Is.EqualTo("Bearer test-jwt"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Env-Id"), Is.EqualTo("test-env"));
        Assert.That(queryRequest.GetHeader("User-Agent"), Is.EqualTo("chalk-csharp"));
        Assert.That(queryRequest.GetHeader("X-Chalk-Query-Name"), Is.EqualTo("bulk-test"));
    }
}

[TestFixture]
public class BinaryProtocolTests
{
    [Test]
    public void ByteBaseModel_RoundTrip()
    {
        var attrs = """{"key": "value"}""";
        var section1Data = new byte[] { 10, 20, 30, 40, 50 };
        var section2Data = new byte[] { 60, 70, 80 };

        var encoded = BinaryProtocol.BuildByteBaseModel(attrs, new[]
        {
            ("section1", section1Data),
            ("section2", section2Data)
        });

        // Verify magic bytes
        var magic = Encoding.ASCII.GetString(encoded, 0, 23);
        Assert.That(magic, Is.EqualTo("CHALK_BYTE_TRANSMISSION"));

        // Round-trip parse
        var (parsedAttrs, parsedSections) = BinaryProtocol.ParseByteBaseModel(encoded);
        Assert.That(parsedAttrs, Is.EqualTo(attrs));
        Assert.That(parsedSections, Has.Count.EqualTo(2));
        Assert.That(parsedSections["section1"], Is.EqualTo(section1Data));
        Assert.That(parsedSections["section2"], Is.EqualTo(section2Data));
    }

    [Test]
    public void FeatherRequest_HasCorrectFormat()
    {
        var headerJson = """{"outputs": ["user.name"]}""";
        var featherBytes = new byte[] { 1, 2, 3, 4 };

        var encoded = BinaryProtocol.BuildFeatherRequest(headerJson, featherBytes);

        // Verify magic
        var magic = Encoding.ASCII.GetString(encoded, 0, 5);
        Assert.That(magic, Is.EqualTo("chal1"));

        // Verify total length is magic(5) + len(8) + header + len(8) + feather
        var headerBytesLen = Encoding.UTF8.GetByteCount(headerJson);
        var expectedLen = 5 + 8 + headerBytesLen + 8 + featherBytes.Length;
        Assert.That(encoded.Length, Is.EqualTo(expectedLen));
    }

    [Test]
    public void ByteBaseModel_EmptySections()
    {
        var attrs = "{}";
        var encoded = BinaryProtocol.BuildByteBaseModel(attrs, Array.Empty<(string, byte[])>());

        var (parsedAttrs, parsedSections) = BinaryProtocol.ParseByteBaseModel(encoded);
        Assert.That(parsedAttrs, Is.EqualTo(attrs));
        Assert.That(parsedSections, Is.Empty);
    }
}

[TestFixture]
public class ArrowConverterTests
{
    [Test]
    public void InputsToFeatherBytes_IntColumns()
    {
        var inputs = new Dictionary<string, IList<object?>>
        {
            ["id"] = new List<object?> { 1, 2, 3 }
        };

        var bytes = ArrowConverter.InputsToFeatherBytes(inputs);

        // Verify it produces valid bytes (Arrow IPC files start with "ARROW1" magic)
        Assert.That(bytes.Length, Is.GreaterThan(0));
        var magic = Encoding.ASCII.GetString(bytes, 0, 6);
        Assert.That(magic, Is.EqualTo("ARROW1"));
    }

    [Test]
    public void InputsToFeatherBytes_StringColumns()
    {
        var inputs = new Dictionary<string, IList<object?>>
        {
            ["name"] = new List<object?> { "Alice", "Bob", null }
        };

        var bytes = ArrowConverter.InputsToFeatherBytes(inputs);
        Assert.That(bytes.Length, Is.GreaterThan(0));
    }

    [Test]
    public void InputsToFeatherBytes_MixedColumns()
    {
        var inputs = new Dictionary<string, IList<object?>>
        {
            ["id"] = new List<object?> { 1, 2, 3 },
            ["name"] = new List<object?> { "Alice", "Bob", "Charlie" },
            ["score"] = new List<object?> { 1.5, 2.5, 3.5 },
            ["active"] = new List<object?> { true, false, true }
        };

        var bytes = ArrowConverter.InputsToFeatherBytes(inputs);
        Assert.That(bytes.Length, Is.GreaterThan(0));
        var magic = Encoding.ASCII.GetString(bytes, 0, 6);
        Assert.That(magic, Is.EqualTo("ARROW1"));
    }

    [Test]
    public void InputsToFeatherBytes_EmptyInputs_Throws()
    {
        var inputs = new Dictionary<string, IList<object?>>();

        Assert.Throws<ArgumentException>(() => ArrowConverter.InputsToFeatherBytes(inputs));
    }
}
