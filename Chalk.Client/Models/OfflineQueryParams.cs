using Newtonsoft.Json;

namespace Chalk.Models;

/// <summary>
/// Input for an offline query, in columnar format.
/// </summary>
public class OfflineQueryInput
{
    [JsonProperty("columns")]
    public List<string> Columns { get; set; } = new();

    [JsonProperty("values")]
    public List<List<object?>> Values { get; set; } = new();
}

/// <summary>
/// Parameters for an offline query.
/// </summary>
public class OfflineQueryParams
{
    [JsonProperty("input")]
    public OfflineQueryInput? Input { get; set; }

    [JsonProperty("output")]
    public List<string> Outputs { get; set; } = new();

    [JsonProperty("dataset_name")]
    public string? DatasetName { get; set; }

    [JsonProperty("branch")]
    public string? Branch { get; set; }

    [JsonProperty("max_samples")]
    public int? MaxSamples { get; set; }

    [JsonProperty("observed_at_lower_bound")]
    public DateTimeOffset? ObservedAtLowerBound { get; set; }

    [JsonProperty("observed_at_upper_bound")]
    public DateTimeOffset? ObservedAtUpperBound { get; set; }

    [JsonProperty("tags")]
    public List<string>? Tags { get; set; }

    [JsonProperty("required_resolver_tags")]
    public List<string>? RequiredResolverTags { get; set; }

    [JsonProperty("correlation_id")]
    public string? CorrelationId { get; set; }

    [JsonProperty("store_online")]
    public bool? StoreOnline { get; set; }

    [JsonProperty("store_offline")]
    public bool? StoreOffline { get; set; }

    [JsonProperty("run_asynchronously")]
    public bool? RunAsynchronously { get; set; }

    [JsonProperty("max_cache_age_secs")]
    public int? MaxCacheAgeSecs { get; set; }

    [JsonProperty("recompute_features")]
    public bool? RecomputeFeatures { get; set; }

    [JsonProperty("planner_options")]
    public Dictionary<string, object>? PlannerOptions { get; set; }

    [JsonProperty("query_context")]
    public Dictionary<string, object>? QueryContext { get; set; }

    [JsonProperty("query_name")]
    public string? QueryName { get; set; }

    [JsonProperty("query_name_version")]
    public string? QueryNameVersion { get; set; }

    [JsonProperty("store_plan_stages")]
    public bool? StorePlanStages { get; set; }

    [JsonProperty("explain")]
    public bool? Explain { get; set; }

    [JsonProperty("num_shards")]
    public int? NumShards { get; set; }

    [JsonProperty("num_workers")]
    public int? NumWorkers { get; set; }

    [JsonProperty("completion_deadline")]
    public DateTimeOffset? CompletionDeadline { get; set; }

    [JsonProperty("max_retries")]
    public int? MaxRetries { get; set; }
}

/// <summary>
/// Builder for OfflineQueryParams with fluent API.
/// </summary>
public class OfflineQueryParamsBuilder
{
    private readonly OfflineQueryParams _params = new();
    private readonly Dictionary<string, List<object?>> _inputs = new();

    /// <summary>
    /// Add an input feature column with values.
    /// </summary>
    public OfflineQueryParamsBuilder WithInput(string featureFqn, IList<object?> values)
    {
        _inputs[featureFqn] = new List<object?>(values);
        return this;
    }

    /// <summary>
    /// Add output features to request.
    /// </summary>
    public OfflineQueryParamsBuilder WithOutputs(params string[] outputs)
    {
        _params.Outputs.AddRange(outputs);
        return this;
    }

    /// <summary>
    /// Add output features to request.
    /// </summary>
    public OfflineQueryParamsBuilder WithOutputs(IEnumerable<string> outputs)
    {
        _params.Outputs.AddRange(outputs);
        return this;
    }

    /// <summary>
    /// Set the dataset name.
    /// </summary>
    public OfflineQueryParamsBuilder WithDatasetName(string datasetName)
    {
        _params.DatasetName = datasetName;
        return this;
    }

    /// <summary>
    /// Set the branch.
    /// </summary>
    public OfflineQueryParamsBuilder WithBranch(string branch)
    {
        _params.Branch = branch;
        return this;
    }

    /// <summary>
    /// Set the max samples.
    /// </summary>
    public OfflineQueryParamsBuilder WithMaxSamples(int maxSamples)
    {
        _params.MaxSamples = maxSamples;
        return this;
    }

    /// <summary>
    /// Set the observed-at lower bound.
    /// </summary>
    public OfflineQueryParamsBuilder WithObservedAtLowerBound(DateTimeOffset lowerBound)
    {
        _params.ObservedAtLowerBound = lowerBound;
        return this;
    }

    /// <summary>
    /// Set the observed-at upper bound.
    /// </summary>
    public OfflineQueryParamsBuilder WithObservedAtUpperBound(DateTimeOffset upperBound)
    {
        _params.ObservedAtUpperBound = upperBound;
        return this;
    }

    /// <summary>
    /// Add tags.
    /// </summary>
    public OfflineQueryParamsBuilder WithTags(params string[] tags)
    {
        _params.Tags ??= new();
        _params.Tags.AddRange(tags);
        return this;
    }

    /// <summary>
    /// Set required resolver tags.
    /// </summary>
    public OfflineQueryParamsBuilder WithRequiredResolverTags(params string[] tags)
    {
        _params.RequiredResolverTags ??= new();
        _params.RequiredResolverTags.AddRange(tags);
        return this;
    }

    /// <summary>
    /// Set the correlation ID.
    /// </summary>
    public OfflineQueryParamsBuilder WithCorrelationId(string correlationId)
    {
        _params.CorrelationId = correlationId;
        return this;
    }

    /// <summary>
    /// Set whether to store results online.
    /// </summary>
    public OfflineQueryParamsBuilder WithStoreOnline(bool storeOnline = true)
    {
        _params.StoreOnline = storeOnline;
        return this;
    }

    /// <summary>
    /// Set whether to store results offline.
    /// </summary>
    public OfflineQueryParamsBuilder WithStoreOffline(bool storeOffline = true)
    {
        _params.StoreOffline = storeOffline;
        return this;
    }

    /// <summary>
    /// Set whether to run asynchronously.
    /// </summary>
    public OfflineQueryParamsBuilder WithRunAsynchronously(bool runAsync = true)
    {
        _params.RunAsynchronously = runAsync;
        return this;
    }

    /// <summary>
    /// Set max cache age in seconds.
    /// </summary>
    public OfflineQueryParamsBuilder WithMaxCacheAgeSecs(int maxCacheAgeSecs)
    {
        _params.MaxCacheAgeSecs = maxCacheAgeSecs;
        return this;
    }

    /// <summary>
    /// Set whether to recompute features.
    /// </summary>
    public OfflineQueryParamsBuilder WithRecomputeFeatures(bool recompute = true)
    {
        _params.RecomputeFeatures = recompute;
        return this;
    }

    /// <summary>
    /// Set planner options.
    /// </summary>
    public OfflineQueryParamsBuilder WithPlannerOptions(Dictionary<string, object> options)
    {
        _params.PlannerOptions ??= new();
        foreach (var (key, value) in options)
        {
            _params.PlannerOptions[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Set query context.
    /// </summary>
    public OfflineQueryParamsBuilder WithQueryContext(Dictionary<string, object> context)
    {
        _params.QueryContext ??= new();
        foreach (var (key, value) in context)
        {
            _params.QueryContext[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Set the query name.
    /// </summary>
    public OfflineQueryParamsBuilder WithQueryName(string queryName)
    {
        _params.QueryName = queryName;
        return this;
    }

    /// <summary>
    /// Set the query name version.
    /// </summary>
    public OfflineQueryParamsBuilder WithQueryNameVersion(string queryNameVersion)
    {
        _params.QueryNameVersion = queryNameVersion;
        return this;
    }

    /// <summary>
    /// Set whether to store plan stages.
    /// </summary>
    public OfflineQueryParamsBuilder WithStorePlanStages(bool storePlanStages = true)
    {
        _params.StorePlanStages = storePlanStages;
        return this;
    }

    /// <summary>
    /// Set whether to explain the query.
    /// </summary>
    public OfflineQueryParamsBuilder WithExplain(bool explain = true)
    {
        _params.Explain = explain;
        return this;
    }

    /// <summary>
    /// Set number of shards.
    /// </summary>
    public OfflineQueryParamsBuilder WithNumShards(int numShards)
    {
        _params.NumShards = numShards;
        return this;
    }

    /// <summary>
    /// Set number of workers.
    /// </summary>
    public OfflineQueryParamsBuilder WithNumWorkers(int numWorkers)
    {
        _params.NumWorkers = numWorkers;
        return this;
    }

    /// <summary>
    /// Set the completion deadline.
    /// </summary>
    public OfflineQueryParamsBuilder WithCompletionDeadline(DateTimeOffset deadline)
    {
        _params.CompletionDeadline = deadline;
        return this;
    }

    /// <summary>
    /// Set max retries.
    /// </summary>
    public OfflineQueryParamsBuilder WithMaxRetries(int maxRetries)
    {
        _params.MaxRetries = maxRetries;
        return this;
    }

    /// <summary>
    /// Build the OfflineQueryParams.
    /// </summary>
    public OfflineQueryParams Build()
    {
        if (_params.Outputs.Count == 0)
        {
            throw new InvalidOperationException("At least one output is required");
        }

        if (_inputs.Count > 0)
        {
            var input = new OfflineQueryInput();
            var columns = _inputs.Keys.ToList();
            input.Columns = columns;

            // Determine row count from first column
            var rowCount = _inputs[columns[0]].Count;

            // Build row-oriented values from columnar inputs
            for (var row = 0; row < rowCount; row++)
            {
                var rowValues = new List<object?>();
                foreach (var col in columns)
                {
                    rowValues.Add(_inputs[col][row]);
                }
                input.Values.Add(rowValues);
            }

            _params.Input = input;
        }

        return _params;
    }
}
