using System.Net;
using System.Text;

namespace Chalk.Client.Tests;

/// <summary>
/// A mock HTTP message handler that intercepts requests and returns pre-configured
/// responses. This is the C# equivalent of Rust's mockito library — responses are
/// matched by HTTP method and URL path, consumed in FIFO order.
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
        _responses.Add(new MockedResponse(method.Method, path, statusCode, responseBody));
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var bodyContent = request.Content != null
            ? await request.Content.ReadAsStringAsync(cancellationToken)
            : null;

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in request.Headers)
        {
            headers[header.Key] = string.Join(", ", header.Value);
        }

        _capturedRequests.Add(new CapturedRequest(
            request.Method,
            request.RequestUri!,
            headers,
            bodyContent));

        var requestPath = request.RequestUri!.AbsolutePath;
        var requestMethod = request.Method.Method;

        for (var i = 0; i < _responses.Count; i++)
        {
            var mock = _responses[i];
            if (string.Equals(mock.Method, requestMethod, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(mock.Path, requestPath, StringComparison.OrdinalIgnoreCase))
            {
                _responses.RemoveAt(i);
                return new HttpResponseMessage(mock.StatusCode)
                {
                    Content = new StringContent(mock.Body, Encoding.UTF8, "application/json")
                };
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
        string Body);
}

/// <summary>
/// A captured HTTP request with helpers for test assertions.
/// </summary>
internal sealed record CapturedRequest(
    HttpMethod Method,
    Uri Uri,
    Dictionary<string, string> Headers,
    string? Body)
{
    /// <summary>
    /// Get a header value by name (case-insensitive). Returns null if not present.
    /// </summary>
    public string? GetHeader(string name) =>
        Headers.TryGetValue(name, out var value) ? value : null;
}
