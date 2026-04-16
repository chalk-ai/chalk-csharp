using System.Net.Http.Headers;
using System.Text;
using Chalk.Config;
using Chalk.Exceptions;
using Chalk.Internal;
using Chalk.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Chalk;

/// <summary>
/// HTTP-based Chalk client implementation.
/// </summary>
public class ChalkClient : IChalkClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly SourcedConfig _apiServer;
    private readonly SourcedConfig _clientId;
    private readonly SourcedConfig _clientSecret;
    private readonly SourcedConfig _environmentId;
    private readonly string? _branch;
    private readonly string? _deploymentTag;
    private readonly string? _queryServerOverride;
    private readonly TimeSpan? _timeout;

    private JwtToken? _jwt;
    private Dictionary<string, Uri> _engines = new();

    private static readonly JsonSerializerSettings JsonSettings = new()
    {
        ContractResolver = new DefaultContractResolver
        {
            NamingStrategy = new SnakeCaseNamingStrategy()
        },
        NullValueHandling = NullValueHandling.Ignore
    };

    /// <summary>
    /// Create a new ChalkClient using default configuration.
    /// </summary>
    public static IChalkClient Create()
    {
        return new ChalkClientBuilder().Build();
    }

    /// <summary>
    /// Create a new ChalkClient builder.
    /// </summary>
    public static ChalkClientBuilder Builder()
    {
        return new ChalkClientBuilder();
    }

    /// <summary>
    /// Create a new gRPC-based ChalkClient.
    /// </summary>
    public static IChalkClient CreateGrpc()
    {
        return new ChalkClientBuilder().WithGrpc().Build();
    }

    internal ChalkClient(ChalkClientBuilder builder)
    {
        var yamlConfig = ConfigLoader.LoadChalkYamlConfig();

        _apiServer = ConfigLoader.GetConfig(
            builder.ApiServer,
            ConfigEnvVars.ApiServer,
            yamlConfig?.ApiServer,
            ConfigLoader.GetDefaultApiServer());

        _clientId = ConfigLoader.GetConfig(
            builder.ClientId,
            ConfigEnvVars.ClientId,
            yamlConfig?.ClientId);

        _clientSecret = ConfigLoader.GetConfig(
            builder.ClientSecret,
            ConfigEnvVars.ClientSecret,
            yamlConfig?.ClientSecret);

        _environmentId = ConfigLoader.GetConfig(
            builder.EnvironmentId,
            ConfigEnvVars.Environment,
            yamlConfig?.ActiveEnvironment);

        _branch = builder.Branch ?? Environment.GetEnvironmentVariable(ConfigEnvVars.Branch);
        _deploymentTag = builder.DeploymentTag ?? Environment.GetEnvironmentVariable(ConfigEnvVars.DeploymentTag);
        _queryServerOverride = builder.QueryServer ?? Environment.GetEnvironmentVariable(ConfigEnvVars.QueryServer);
        _timeout = builder.Timeout;

        if (builder.HttpClient != null)
        {
            _httpClient = builder.HttpClient;
            _ownsHttpClient = false;
        }
        else
        {
            _httpClient = new HttpClient();
            _ownsHttpClient = true;
        }

        ValidateConfig();
    }

    private void ValidateConfig()
    {
        if (_clientId.IsEmpty || _clientSecret.IsEmpty || _apiServer.IsEmpty)
        {
            PrintConfig();
            throw new ClientException("Chalk's config variables are not set correctly. See output for details.");
        }
    }

    // ----------------------------------------------------------------
    // Online Query
    // ----------------------------------------------------------------

    public async Task<OnlineQueryResult> OnlineQueryAsync(OnlineQueryParams queryParams, CancellationToken cancellationToken = default)
    {
        await RefreshJwtAsync(false, cancellationToken);

        var requestBody = BuildOnlineQueryRequest(queryParams);
        var jsonBody = JsonConvert.SerializeObject(requestBody, JsonSettings);

        var request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/query/online"))
        {
            Content = new StringContent(jsonBody, Encoding.UTF8, "application/json")
        };

        AddHeaders(request, queryParams);

        var timeout = queryParams.Timeout ?? _timeout;
        using var cts = timeout.HasValue
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;

        if (cts != null)
        {
            cts.CancelAfter(timeout!.Value);
        }

        var response = await _httpClient.SendAsync(request, cts?.Token ?? cancellationToken);

        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
        {
            // Retry with fresh token
            await RefreshJwtAsync(true, cancellationToken);
            request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/query/online"))
            {
                Content = new StringContent(jsonBody, Encoding.UTF8, "application/json")
            };
            AddHeaders(request, queryParams);
            response = await _httpClient.SendAsync(request, cts?.Token ?? cancellationToken);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw ParseHttpException(response.StatusCode, responseBody);
        }

        return ParseOnlineQueryResponse(responseBody);
    }

    public OnlineQueryResult OnlineQuery(OnlineQueryParams queryParams)
    {
        return OnlineQueryAsync(queryParams).GetAwaiter().GetResult();
    }

    // ----------------------------------------------------------------
    // Offline Query
    // ----------------------------------------------------------------

    public async Task<OfflineQueryResult> OfflineQueryAsync(OfflineQueryParams queryParams, CancellationToken cancellationToken = default)
    {
        await RefreshJwtAsync(false, cancellationToken);

        var jsonBody = JsonConvert.SerializeObject(queryParams, JsonSettings);

        var request = new HttpRequestMessage(HttpMethod.Post, GetApiServerUri("/v4/offline_query"))
        {
            Content = new StringContent(jsonBody, Encoding.UTF8, "application/json")
        };

        AddHeaders(request, branch: queryParams.Branch, queryName: queryParams.QueryName);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
        {
            await RefreshJwtAsync(true, cancellationToken);
            request = new HttpRequestMessage(HttpMethod.Post, GetApiServerUri("/v4/offline_query"))
            {
                Content = new StringContent(jsonBody, Encoding.UTF8, "application/json")
            };
            AddHeaders(request, branch: queryParams.Branch, queryName: queryParams.QueryName);
            response = await _httpClient.SendAsync(request, cancellationToken);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw ParseHttpException(response.StatusCode, responseBody);
        }

        var result = JsonConvert.DeserializeObject<OfflineQueryResult>(responseBody, JsonSettings);
        if (result == null)
        {
            throw new ClientException("Failed to parse offline query response");
        }

        return result;
    }

    public OfflineQueryResult OfflineQuery(OfflineQueryParams queryParams)
    {
        return OfflineQueryAsync(queryParams).GetAwaiter().GetResult();
    }

    public async Task<OfflineQueryStatusResult> GetOfflineQueryStatusAsync(string revisionId, CancellationToken cancellationToken = default)
    {
        await RefreshJwtAsync(false, cancellationToken);

        var request = new HttpRequestMessage(HttpMethod.Get, GetApiServerUri($"/v4/offline_query/{revisionId}/status"));
        AddHeaders(request);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
        {
            await RefreshJwtAsync(true, cancellationToken);
            request = new HttpRequestMessage(HttpMethod.Get, GetApiServerUri($"/v4/offline_query/{revisionId}/status"));
            AddHeaders(request);
            response = await _httpClient.SendAsync(request, cancellationToken);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw ParseHttpException(response.StatusCode, responseBody);
        }

        var result = JsonConvert.DeserializeObject<OfflineQueryStatusResult>(responseBody, JsonSettings);
        if (result == null)
        {
            throw new ClientException("Failed to parse offline query status response");
        }

        return result;
    }

    public async Task<OfflineQueryResult> WaitForOfflineQueryAsync(OfflineQueryResult result, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var revisionId = result.Revisions?.FirstOrDefault()?.RevisionId
                         ?? result.DatasetRevisionId;

        if (string.IsNullOrEmpty(revisionId))
        {
            throw new ClientException("No revision ID found in offline query result");
        }

        var deadline = timeout.HasValue ? DateTimeOffset.UtcNow.Add(timeout.Value) : (DateTimeOffset?)null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (deadline.HasValue && DateTimeOffset.UtcNow >= deadline.Value)
            {
                throw new ClientException($"Timed out waiting for offline query {revisionId}");
            }

            var status = await GetOfflineQueryStatusAsync(revisionId, cancellationToken);
            var statusStr = status.Report?.Status?.ToLowerInvariant();

            if (statusStr is "succeeded" or "failed" or "cancelled")
            {
                if (statusStr == "failed")
                {
                    var errorMsg = status.Report?.Error ?? "Unknown error";
                    throw new ClientException($"Offline query {revisionId} failed: {errorMsg}");
                }

                return result;
            }

            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        }
    }

    public async Task<List<string>> GetOfflineQueryDownloadUrlsAsync(OfflineQueryResult result, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var revisionId = result.Revisions?.FirstOrDefault()?.RevisionId
                         ?? result.DatasetRevisionId;

        if (string.IsNullOrEmpty(revisionId))
        {
            throw new ClientException("No revision ID found in offline query result");
        }

        var deadline = timeout.HasValue ? DateTimeOffset.UtcNow.Add(timeout.Value) : (DateTimeOffset?)null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (deadline.HasValue && DateTimeOffset.UtcNow >= deadline.Value)
            {
                throw new ClientException($"Timed out waiting for offline query download URLs for {revisionId}");
            }

            await RefreshJwtAsync(false, cancellationToken);

            var request = new HttpRequestMessage(HttpMethod.Get, GetApiServerUri($"/v2/offline_query/{revisionId}"));
            AddHeaders(request);

            var response = await _httpClient.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw ParseHttpException(response.StatusCode, responseBody);
            }

            var downloadResult = JsonConvert.DeserializeObject<OfflineQueryDownloadResult>(responseBody, JsonSettings);
            if (downloadResult == null)
            {
                throw new ClientException("Failed to parse offline query download response");
            }

            if (downloadResult.IsFinished)
            {
                return downloadResult.Urls ?? new List<string>();
            }

            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        }
    }

    // ----------------------------------------------------------------
    // Upload Features
    // ----------------------------------------------------------------

    public async Task<UploadFeaturesResult> UploadFeaturesAsync(Dictionary<string, IList<object?>> inputs, CancellationToken cancellationToken = default)
    {
        if (inputs.Count == 0)
        {
            throw new ClientException("Inputs must not be empty for upload_features");
        }

        await RefreshJwtAsync(false, cancellationToken);

        var featherBytes = ArrowConverter.InputsToFeatherBytes(inputs);
        var columns = inputs.Keys.ToList();
        var attrsJson = JsonConvert.SerializeObject(new { columns }, JsonSettings);
        var body = BinaryProtocol.BuildByteBaseModel(attrsJson, new[] { ("table_bytes", featherBytes) });

        var request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/upload_features/multi"))
        {
            Content = new ByteArrayContent(body)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        AddHeaders(request);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
        {
            await RefreshJwtAsync(true, cancellationToken);
            request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/upload_features/multi"))
            {
                Content = new ByteArrayContent(body)
            };
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            AddHeaders(request);
            response = await _httpClient.SendAsync(request, cancellationToken);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw ParseHttpException(response.StatusCode, responseBody);
        }

        var result = JsonConvert.DeserializeObject<UploadFeaturesResult>(responseBody, JsonSettings);
        return result ?? new UploadFeaturesResult();
    }

    public UploadFeaturesResult UploadFeatures(Dictionary<string, IList<object?>> inputs)
    {
        return UploadFeaturesAsync(inputs).GetAwaiter().GetResult();
    }

    // ----------------------------------------------------------------
    // Bulk Query
    // ----------------------------------------------------------------

    public async Task<BulkQueryResult> OnlineQueryBulkAsync(OnlineQueryParams queryParams, CancellationToken cancellationToken = default)
    {
        await RefreshJwtAsync(false, cancellationToken);

        var featherBytes = ArrowConverter.InputsToFeatherBytes(queryParams.Inputs);

        var header = new Dictionary<string, object>
        {
            ["outputs"] = queryParams.Outputs
        };

        if (queryParams.Staleness != null)
        {
            header["staleness"] = queryParams.Staleness.ToDictionary(
                kv => kv.Key,
                kv => $"{kv.Value.TotalSeconds}s");
        }

        if (queryParams.Now != null)
        {
            header["now"] = queryParams.Now.Select(n => n.ToString("o")).ToList();
        }

        if (!string.IsNullOrEmpty(queryParams.CorrelationId))
        {
            header["correlation_id"] = queryParams.CorrelationId;
        }

        if (queryParams.IncludeMeta)
        {
            header["include_meta"] = true;
        }

        if (queryParams.StorePlanStages)
        {
            header["store_plan_stages"] = true;
        }

        if (queryParams.Explain)
        {
            header["explain"] = true;
        }

        if (queryParams.Tags != null)
        {
            header["tags"] = queryParams.Tags;
        }

        if (queryParams.RequiredResolverTags != null)
        {
            header["required_resolver_tags"] = queryParams.RequiredResolverTags;
        }

        if (queryParams.PlannerOptions != null)
        {
            header["planner_options"] = queryParams.PlannerOptions;
        }

        if (queryParams.Meta != null)
        {
            header["meta"] = queryParams.Meta;
        }

        if (!string.IsNullOrEmpty(queryParams.QueryName))
        {
            header["query_name"] = queryParams.QueryName;
        }

        if (!string.IsNullOrEmpty(queryParams.QueryNameVersion))
        {
            header["query_name_version"] = queryParams.QueryNameVersion;
        }

        var headerJson = JsonConvert.SerializeObject(header, JsonSettings);
        var body = BinaryProtocol.BuildFeatherRequest(headerJson, featherBytes);

        var request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/query/feather"))
        {
            Content = new ByteArrayContent(body)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        AddHeaders(request, queryParams);

        var timeout = queryParams.Timeout ?? _timeout;
        using var cts = timeout.HasValue
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;

        if (cts != null)
        {
            cts.CancelAfter(timeout!.Value);
        }

        var response = await _httpClient.SendAsync(request, cts?.Token ?? cancellationToken);

        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
        {
            await RefreshJwtAsync(true, cancellationToken);
            request = new HttpRequestMessage(HttpMethod.Post, GetQueryUri("/v1/query/feather"))
            {
                Content = new ByteArrayContent(body)
            };
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            AddHeaders(request, queryParams);
            response = await _httpClient.SendAsync(request, cts?.Token ?? cancellationToken);
        }

        var responseBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var errorBody = Encoding.UTF8.GetString(responseBytes);
            throw ParseHttpException(response.StatusCode, errorBody);
        }

        return ParseBulkQueryResponse(responseBytes);
    }

    public BulkQueryResult OnlineQueryBulk(OnlineQueryParams queryParams)
    {
        return OnlineQueryBulkAsync(queryParams).GetAwaiter().GetResult();
    }

    // ----------------------------------------------------------------
    // Config
    // ----------------------------------------------------------------

    public void PrintConfig()
    {
        Console.WriteLine("ChalkClient Configuration:");
        Console.WriteLine($"  API Server:     {_apiServer.Value} (from {_apiServer.Source})");
        Console.WriteLine($"  Environment ID: {_environmentId.Value} (from {_environmentId.Source})");
        Console.WriteLine($"  Client ID:      {_clientId.Value} (from {_clientId.Source})");
        Console.WriteLine($"  Client Secret:  {new string('*', _clientSecret.Value.Length)} (from {_clientSecret.Source})");
        Console.WriteLine();
        Console.WriteLine("Configuration precedence: builder > environment variable > chalk.yaml > default");
    }

    // ----------------------------------------------------------------
    // Request building
    // ----------------------------------------------------------------

    private object BuildOnlineQueryRequest(OnlineQueryParams queryParams)
    {
        // Convert inputs from List<object?> to single values for single-row queries
        // The /v1/query/online endpoint expects single values, not arrays
        var inputs = new Dictionary<string, object?>();
        foreach (var (key, values) in queryParams.Inputs)
        {
            if (values.Count == 1)
            {
                // Single-row query - use single value
                inputs[key] = values[0];
            }
            else
            {
                // Multi-row query - keep as array
                inputs[key] = values;
            }
        }

        var request = new Dictionary<string, object>
        {
            ["inputs"] = inputs,
            ["outputs"] = queryParams.Outputs
        };

        if (queryParams.Staleness != null)
        {
            request["staleness"] = queryParams.Staleness.ToDictionary(
                kv => kv.Key,
                kv => $"{kv.Value.TotalSeconds}s");
        }

        if (queryParams.Meta != null)
        {
            request["meta"] = queryParams.Meta;
        }

        if (queryParams.Tags != null)
        {
            request["tags"] = queryParams.Tags;
        }

        if (queryParams.IncludeMeta)
        {
            request["include_meta"] = true;
        }

        if (queryParams.StorePlanStages)
        {
            request["store_plan_stages"] = true;
        }

        if (queryParams.Explain)
        {
            request["explain"] = true;
        }

        if (queryParams.Now != null)
        {
            request["now"] = queryParams.Now.Select(n => n.ToString("o")).ToList();
        }

        if (queryParams.RequiredResolverTags != null)
        {
            request["required_resolver_tags"] = queryParams.RequiredResolverTags;
        }

        if (queryParams.PlannerOptions != null)
        {
            request["planner_options"] = queryParams.PlannerOptions;
        }

        if (!string.IsNullOrEmpty(queryParams.CorrelationId))
        {
            request["correlation_id"] = queryParams.CorrelationId;
        }

        if (!string.IsNullOrEmpty(queryParams.QueryName))
        {
            request["query_name"] = queryParams.QueryName;
        }

        if (!string.IsNullOrEmpty(queryParams.QueryNameVersion))
        {
            request["query_name_version"] = queryParams.QueryNameVersion;
        }

        return request;
    }

    // ----------------------------------------------------------------
    // Headers
    // ----------------------------------------------------------------

    private void AddHeaders(HttpRequestMessage request, OnlineQueryParams queryParams)
    {
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("User-Agent", "chalk-csharp");
        request.Headers.Add("X-Chalk-Client-Id", _clientId.Value);

        if (_jwt != null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _jwt.Value);
        }

        var envId = queryParams.EnvironmentId ?? _environmentId.Value;
        if (!string.IsNullOrEmpty(envId))
        {
            request.Headers.Add("X-Chalk-Env-Id", envId);
        }

        var branch = queryParams.Branch ?? _branch;
        if (!string.IsNullOrEmpty(branch))
        {
            request.Headers.Add("X-Chalk-Branch-Id", branch);
            request.Headers.Add("X-Chalk-Deployment-Type", "branch");
        }
        else
        {
            request.Headers.Add("X-Chalk-Deployment-Type", "engine");
        }

        if (!string.IsNullOrEmpty(queryParams.PreviewDeploymentId))
        {
            request.Headers.Add("X-Chalk-Preview-Deployment", queryParams.PreviewDeploymentId);
        }

        if (!string.IsNullOrEmpty(_deploymentTag))
        {
            request.Headers.Add("X-Chalk-Deployment-Tag", _deploymentTag);
        }

        if (!string.IsNullOrEmpty(queryParams.QueryName))
        {
            request.Headers.Add("X-Chalk-Query-Name", queryParams.QueryName);
        }
    }

    private void AddHeaders(HttpRequestMessage request, string? branch = null, string? queryName = null)
    {
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("User-Agent", "chalk-csharp");
        request.Headers.Add("X-Chalk-Client-Id", _clientId.Value);

        if (_jwt != null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _jwt.Value);
        }

        if (!string.IsNullOrEmpty(_environmentId.Value))
        {
            request.Headers.Add("X-Chalk-Env-Id", _environmentId.Value);
        }

        var effectiveBranch = branch ?? _branch;
        if (!string.IsNullOrEmpty(effectiveBranch))
        {
            request.Headers.Add("X-Chalk-Branch-Id", effectiveBranch);
            request.Headers.Add("X-Chalk-Deployment-Type", "branch");
        }
        else
        {
            request.Headers.Add("X-Chalk-Deployment-Type", "engine");
        }

        if (!string.IsNullOrEmpty(_deploymentTag))
        {
            request.Headers.Add("X-Chalk-Deployment-Tag", _deploymentTag);
        }

        if (!string.IsNullOrEmpty(queryName))
        {
            request.Headers.Add("X-Chalk-Query-Name", queryName);
        }
    }

    // ----------------------------------------------------------------
    // URL routing
    // ----------------------------------------------------------------

    private Uri GetQueryUri(string path)
    {
        // Try query server override first
        if (!string.IsNullOrEmpty(_queryServerOverride))
        {
            return new Uri(new Uri(_queryServerOverride), path);
        }

        // Try environment-specific engine
        var envId = _environmentId.Value;
        if (!string.IsNullOrEmpty(envId) && _engines.TryGetValue(envId, out var engineUri))
        {
            return new Uri(engineUri, path);
        }

        // Fall back to API server
        return new Uri(new Uri(_apiServer.Value), path);
    }

    private Uri GetApiServerUri(string path)
    {
        return new Uri(new Uri(_apiServer.Value), path);
    }

    // ----------------------------------------------------------------
    // Token management
    // ----------------------------------------------------------------

    private async Task RefreshJwtAsync(bool force, CancellationToken cancellationToken)
    {
        if (!force && _jwt != null && !_jwt.IsExpired)
        {
            return;
        }

        var tokenRequest = new GetTokenRequest
        {
            ClientId = _clientId.Value,
            ClientSecret = _clientSecret.Value,
            GrantType = "client_credentials"
        };

        var jsonBody = JsonConvert.SerializeObject(tokenRequest, JsonSettings);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(new Uri(_apiServer.Value), "/v1/oauth/token"))
        {
            Content = new StringContent(jsonBody, Encoding.UTF8, "application/json")
        };
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("User-Agent", "chalk-csharp");

        var response = await _httpClient.SendAsync(request, cancellationToken);
        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new ClientException($"Failed to get access token: {response.StatusCode} - {responseBody}");
        }

        var tokenResponse = JsonConvert.DeserializeObject<GetTokenResponse>(responseBody, JsonSettings);
        if (tokenResponse == null)
        {
            throw new ClientException("Failed to parse token response");
        }

        var expiry = DateTimeOffset.UtcNow.AddSeconds(tokenResponse.ExpiresIn);
        _jwt = new JwtToken(tokenResponse.AccessToken, expiry);

        // Update engines map
        if (tokenResponse.Engines != null)
        {
            _engines = new Dictionary<string, Uri>();
            foreach (var (key, value) in tokenResponse.Engines)
            {
                try
                {
                    _engines[key] = new Uri(value);
                }
                catch
                {
                    // Ignore invalid URIs
                }
            }
        }

        // Set environment ID from primary environment if not already set
        if (_environmentId.IsEmpty && !string.IsNullOrEmpty(tokenResponse.PrimaryEnvironment))
        {
            // Note: We can't update the readonly field, but we'll use primary env for requests
        }
    }

    // ----------------------------------------------------------------
    // Response parsing
    // ----------------------------------------------------------------

    private OnlineQueryResult ParseOnlineQueryResponse(string responseBody)
    {
        var response = JsonConvert.DeserializeObject<OnlineQueryJsonResponse>(responseBody, JsonSettings);
        if (response == null)
        {
            throw new ClientException("Failed to parse query response");
        }

        var result = new OnlineQueryResult
        {
            Meta = response.Meta,
            Errors = response.Errors ?? new List<ServerError>()
        };

        // Parse data from the response - data is a list of feature results
        if (response.Data != null)
        {
            // Group results by feature field name
            var groupedByField = response.Data
                .Where(r => r.Field != null)
                .GroupBy(r => r.Field!);

            foreach (var group in groupedByField)
            {
                result.Data[group.Key] = group.Select(r => NarrowNumericType(r.Value)).ToList();
            }
        }

        return result;
    }

    private static BulkQueryResult ParseBulkQueryResponse(byte[] responseBytes)
    {
        var result = new BulkQueryResult();

        try
        {
            var (jsonAttrs, sections) = BinaryProtocol.ParseByteBaseModel(responseBytes);
            result.Meta = jsonAttrs;

            if (sections.TryGetValue("table_bytes", out var tableBytes))
            {
                result.ScalarData = tableBytes;
            }

            // Parse errors from the attrs JSON if present
            try
            {
                var attrs = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonAttrs);
                if (attrs != null && attrs.TryGetValue("errors", out var errorsObj) && errorsObj is Newtonsoft.Json.Linq.JArray errorsArray)
                {
                    foreach (var err in errorsArray)
                    {
                        result.Errors.Add(err.ToString());
                    }
                }
            }
            catch
            {
                // Ignore JSON parsing errors for attrs
            }
        }
        catch (Exception ex)
        {
            result.Errors.Add($"Failed to parse bulk query response: {ex.Message}");
        }

        return result;
    }

    /// <summary>
    /// Narrows numeric types returned by JSON deserialization to the smallest
    /// appropriate C# type. Newtonsoft.Json deserializes all JSON integers as
    /// Int64 when the target type is object, which causes issues for consumers
    /// expecting Int32 values (e.g. features defined as int32).
    /// </summary>
    private static object? NarrowNumericType(object? value)
    {
        if (value is long l && l >= int.MinValue && l <= int.MaxValue)
        {
            return (int)l;
        }
        return value;
    }

    private static ChalkException ParseHttpException(System.Net.HttpStatusCode statusCode, string body)
    {
        try
        {
            var error = JsonConvert.DeserializeObject<HttpErrorResponse>(body, JsonSettings);
            if (error != null)
            {
                return new ServerException((int)statusCode, error.Code, error.Message);
            }
        }
        catch
        {
            // Ignore parsing errors
        }

        return new ServerException((int)statusCode, $"HTTP {statusCode}: {body}");
    }

    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }
}

// Internal response models
internal class OnlineQueryJsonResponse
{
    [JsonProperty("data")]
    public List<FeatureResult>? Data { get; set; }

    [JsonProperty("errors")]
    public List<ServerError>? Errors { get; set; }

    [JsonProperty("meta")]
    public QueryMeta? Meta { get; set; }
}

internal class FeatureResult
{
    [JsonProperty("field")]
    public string? Field { get; set; }

    [JsonProperty("value")]
    public object? Value { get; set; }

    [JsonProperty("pkey")]
    public object? Pkey { get; set; }

    [JsonProperty("ts")]
    public DateTimeOffset? Timestamp { get; set; }

    [JsonProperty("valid")]
    public bool Valid { get; set; }

    [JsonProperty("error")]
    public object? Error { get; set; }
}

internal class HttpErrorResponse
{
    [JsonProperty("code")]
    public string? Code { get; set; }

    [JsonProperty("message")]
    public string? Message { get; set; }

    [JsonProperty("detail")]
    public string? Detail { get; set; }
}
