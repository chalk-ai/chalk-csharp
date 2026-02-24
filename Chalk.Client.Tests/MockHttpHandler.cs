using System.Net;
using System.Net.Http.Headers;
using System.Text;

namespace Chalk.Client.Tests;

/// <summary>
/// A mock HTTP message handler that intercepts requests and returns pre-configured
/// responses. Responses are matched by HTTP method and URL path, consumed in FIFO order.
/// All requests are captured for post-hoc assertions on headers, bodies, and URLs.
/// </summary>
internal class MockHttpHandler : HttpMessageHandler
{
    private readonly List<MockedResponse> _responses = [];
    private readonly List<CapturedRequest> _capturedRequests = [];

    /// <summary>
    /// All HTTP requests sent through this handler, in chronological order.
    /// </summary>
    public IReadOnlyList<CapturedRequest> Requests => _capturedRequests;

    /// <summary>
    /// Enqueue a response for the next request matching the given method and path.
    /// If multiple responses are registered for the same (method, path), they are
    /// returned in registration order (first registered, first consumed).
    /// </summary>
    public MockHttpHandler Enqueue(
        HttpMethod method,
        string path,
        HttpStatusCode statusCode,
        string responseBody)
    {
        _responses.Add(new MockedResponse(method.Method, path, statusCode, responseBody, null, "application/json"));
        return this;
    }

    /// <summary>
    /// Enqueue a binary response for the next request matching the given method and path.
    /// </summary>
    public MockHttpHandler EnqueueBinary(
        HttpMethod method,
        string path,
        HttpStatusCode statusCode,
        byte[] responseBytes,
        string contentType = "application/octet-stream")
    {
        _responses.Add(new MockedResponse(method.Method, path, statusCode, null, responseBytes, contentType));
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        byte[]? bodyBytes = null;
        string? bodyContent = null;

        if (request.Content != null)
        {
            bodyBytes = await request.Content.ReadAsByteArrayAsync(cancellationToken);
            bodyContent = Encoding.UTF8.GetString(bodyBytes);
        }

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in request.Headers)
        {
            headers[header.Key] = string.Join(", ", header.Value);
        }

        _capturedRequests.Add(new CapturedRequest(
            request.Method,
            request.RequestUri!,
            headers,
            bodyContent,
            bodyBytes));

        var requestPath = request.RequestUri!.AbsolutePath;
        var requestMethod = request.Method.Method;

        for (var i = 0; i < _responses.Count; i++)
        {
            var mock = _responses[i];
            if (string.Equals(mock.Method, requestMethod, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(mock.Path, requestPath, StringComparison.OrdinalIgnoreCase))
            {
                _responses.RemoveAt(i);

                var responseMessage = new HttpResponseMessage(mock.StatusCode);
                if (mock.BodyBytes != null)
                {
                    responseMessage.Content = new ByteArrayContent(mock.BodyBytes);
                    responseMessage.Content.Headers.ContentType = new MediaTypeHeaderValue(mock.ContentType);
                }
                else
                {
                    responseMessage.Content = new StringContent(mock.Body ?? "", Encoding.UTF8, mock.ContentType);
                }
                return responseMessage;
            }
        }

        return new HttpResponseMessage(HttpStatusCode.NotFound)
        {
            Content = new StringContent(
                $"No mock configured for {requestMethod} {requestPath}",
                Encoding.UTF8,
                "text/plain")
        };
    }

    private sealed record MockedResponse(
        string Method,
        string Path,
        HttpStatusCode StatusCode,
        string? Body,
        byte[]? BodyBytes,
        string ContentType);
}

/// <summary>
/// A captured HTTP request with helpers for test assertions.
/// </summary>
internal sealed record CapturedRequest(
    HttpMethod Method,
    Uri Uri,
    Dictionary<string, string> Headers,
    string? Body,
    byte[]? BodyBytes = null)
{
    /// <summary>
    /// Get a header value by name (case-insensitive). Returns null if not present.
    /// </summary>
    public string? GetHeader(string name) =>
        Headers.TryGetValue(name, out var value) ? value : null;
}
