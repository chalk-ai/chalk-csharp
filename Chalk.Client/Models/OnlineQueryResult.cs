using Newtonsoft.Json;

namespace Chalk.Models;

/// <summary>
/// Result of an online query.
/// </summary>
public class OnlineQueryResult
{
    /// <summary>
    /// The data returned from the query, indexed by feature name.
    /// Each feature maps to a list of values, one per input row.
    /// </summary>
    public Dictionary<string, List<object?>> Data { get; set; } = new();

    /// <summary>
    /// Errors that occurred during the query.
    /// </summary>
    public List<ServerError> Errors { get; set; } = new();

    /// <summary>
    /// Metadata about the query execution.
    /// </summary>
    public QueryMeta? Meta { get; set; }

    /// <summary>
    /// Get a feature value for a single-row query.
    /// </summary>
    public T? GetValue<T>(string featureFqn)
    {
        if (Data.TryGetValue(featureFqn, out var values) && values.Count > 0)
        {
            var value = values[0];
            if (value == null) return default;
            if (value is T typedValue) return typedValue;
            return (T)Convert.ChangeType(value, typeof(T));
        }
        return default;
    }

    /// <summary>
    /// Get all values for a feature.
    /// </summary>
    public List<T?> GetValues<T>(string featureFqn)
    {
        if (Data.TryGetValue(featureFqn, out var values))
        {
            return values.Select(v =>
            {
                if (v == null) return default;
                if (v is T typedValue) return typedValue;
                return (T)Convert.ChangeType(v, typeof(T));
            }).ToList();
        }
        return new List<T?>();
    }
}

/// <summary>
/// Error from the server during query execution.
/// </summary>
public class ServerError
{
    [JsonProperty("code")]
    public string? Code { get; set; }

    [JsonProperty("category")]
    public string? Category { get; set; }

    [JsonProperty("message")]
    public string? Message { get; set; }

    [JsonProperty("feature")]
    public string? Feature { get; set; }

    [JsonProperty("resolver")]
    public string? Resolver { get; set; }

    public override string ToString()
    {
        return $"[{Code}] {Message} (feature: {Feature}, resolver: {Resolver})";
    }
}

/// <summary>
/// Metadata about query execution.
/// </summary>
public class QueryMeta
{
    [JsonProperty("execution_duration_s")]
    public double? ExecutionDurationS { get; set; }

    [JsonProperty("deployment_id")]
    public string? DeploymentId { get; set; }

    [JsonProperty("environment_id")]
    public string? EnvironmentId { get; set; }

    [JsonProperty("environment_name")]
    public string? EnvironmentName { get; set; }

    [JsonProperty("query_id")]
    public string? QueryId { get; set; }

    [JsonProperty("query_hash")]
    public string? QueryHash { get; set; }

    [JsonProperty("trace_id")]
    public string? TraceId { get; set; }

    [JsonProperty("query_timestamp")]
    public DateTimeOffset? QueryTimestamp { get; set; }
}
