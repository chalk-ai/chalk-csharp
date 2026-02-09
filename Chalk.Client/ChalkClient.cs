using System.Net.Http.Headers;
using System.Text;
using Chalk.Config;
using Chalk.Exceptions;
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

        return request;
    }

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
                result.Data[group.Key] = group.Select(r => r.Value).ToList();
            }
        }

        return result;
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
