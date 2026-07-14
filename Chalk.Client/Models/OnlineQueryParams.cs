namespace Chalk.Models;

/// <summary>
/// Parameters for an online query.
/// </summary>
public class OnlineQueryParams
{
    /// <summary>
    /// The features for which there are known values, mapped to those values.
    /// </summary>
    public Dictionary<string, IList<object?>> Inputs { get; set; } = new();

    /// <summary>
    /// Optional schema hints for inputs whose value is type-ambiguous (e.g. an empty has-many),
    /// keyed by input feature fqn. The server uses these to determine the intended column schema
    /// so that, for example, an empty has-many input still plans against the projected columns.
    /// Inputs without a hint are unaffected.
    /// </summary>
    public Dictionary<string, ValueSchemaHint>? InputSchemaHints { get; set; }

    /// <summary>
    /// The features that you'd like to compute from the inputs.
    /// </summary>
    public List<string> Outputs { get; set; } = new();

    /// <summary>
    /// Maximum staleness overrides for any output features or intermediate features.
    /// </summary>
    public Dictionary<string, TimeSpan>? Staleness { get; set; }

    /// <summary>
    /// Metadata to attach to the query.
    /// </summary>
    public Dictionary<string, string>? Meta { get; set; }

    /// <summary>
    /// Tags to attach to the query.
    /// </summary>
    public List<string>? Tags { get; set; }

    /// <summary>
    /// Triggers returning metadata about the query execution.
    /// </summary>
    public bool IncludeMeta { get; set; }

    /// <summary>
    /// If true, the output of each of the query plan stages will be stored.
    /// </summary>
    public bool StorePlanStages { get; set; }

    /// <summary>
    /// Log the query execution plan.
    /// </summary>
    public bool Explain { get; set; }

    /// <summary>
    /// The environment under which to run the resolvers.
    /// </summary>
    public string? EnvironmentId { get; set; }

    /// <summary>
    /// If specified, Chalk will route your request to the relevant preview deployment.
    /// </summary>
    public string? PreviewDeploymentId { get; set; }

    /// <summary>
    /// The semantic name for the query you're making.
    /// </summary>
    public string? QueryName { get; set; }

    /// <summary>
    /// The version of the query name.
    /// </summary>
    public string? QueryNameVersion { get; set; }

    /// <summary>
    /// Correlation ID to be used in logs.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// If specified, Chalk will route your request to the relevant branch.
    /// </summary>
    public string? Branch { get; set; }

    /// <summary>
    /// The time at which to evaluate the query.
    /// </summary>
    public List<DateTimeOffset>? Now { get; set; }

    /// <summary>
    /// Required resolver tags that must be present on a resolver for it to be eligible.
    /// </summary>
    public List<string>? RequiredResolverTags { get; set; }

    /// <summary>
    /// Additional options to pass to the Chalk query engine.
    /// </summary>
    public Dictionary<string, object>? PlannerOptions { get; set; }

    /// <summary>
    /// Timeout for the query.
    /// </summary>
    public TimeSpan? Timeout { get; set; }
}

/// <summary>
/// Builder for OnlineQueryParams with fluent API.
/// </summary>
public class OnlineQueryParamsBuilder
{
    private readonly OnlineQueryParams _params = new();

    /// <summary>
    /// Add an input feature with a list of values (one value per query row).
    /// </summary>
    /// <param name="schema">
    /// Optional schema hint for the input value. Use this when the value is type-ambiguous — most
    /// importantly an empty has-many — so the server can plan against the intended columns.
    /// </param>
    /// <exception cref="ArgumentException">
    /// A <paramref name="schema"/> was provided and one of <paramref name="values"/> does not
    /// match its shape (e.g. a has-many hint on a value that is not a list of dictionary rows,
    /// or a row carrying a column absent from the hint).
    /// </exception>
    public OnlineQueryParamsBuilder WithInput(string featureFqn, IList<object?> values, ValueSchemaHint? schema = null)
    {
        if (schema is not null)
        {
            foreach (var value in values)
            {
                schema.ValidateValue(featureFqn, value);
            }
        }
        _params.Inputs[featureFqn] = values;
        SetInputSchemaHint(featureFqn, schema);
        return this;
    }

    /// <summary>
    /// Add an input feature with a single value (for single-row queries).
    /// </summary>
    /// <param name="schema">
    /// Optional schema hint for the input value. Use this when the value is type-ambiguous — most
    /// importantly an empty has-many — so the server can plan against the intended columns.
    /// </param>
    /// <exception cref="ArgumentException">
    /// A <paramref name="schema"/> was provided and <paramref name="value"/> does not match its
    /// shape (e.g. a has-many hint on a value that is not a list of dictionary rows, or a row
    /// carrying a column absent from the hint).
    /// </exception>
    public OnlineQueryParamsBuilder WithInput(string featureFqn, object? value, ValueSchemaHint? schema = null)
    {
        schema?.ValidateValue(featureFqn, value);
        _params.Inputs[featureFqn] = new List<object?> { value };
        SetInputSchemaHint(featureFqn, schema);
        return this;
    }

    private void SetInputSchemaHint(string featureFqn, ValueSchemaHint? schema)
    {
        if (schema is null)
        {
            return;
        }
        _params.InputSchemaHints ??= new();
        _params.InputSchemaHints[featureFqn] = schema;
    }

    /// <summary>
    /// Add multiple inputs from a dictionary.
    /// </summary>
    public OnlineQueryParamsBuilder WithInputs(Dictionary<string, IList<object?>> inputs)
    {
        foreach (var (key, value) in inputs)
        {
            _params.Inputs[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Add output features to request.
    /// </summary>
    public OnlineQueryParamsBuilder WithOutputs(params string[] outputs)
    {
        _params.Outputs.AddRange(outputs);
        return this;
    }

    /// <summary>
    /// Add output features to request.
    /// </summary>
    public OnlineQueryParamsBuilder WithOutputs(IEnumerable<string> outputs)
    {
        _params.Outputs.AddRange(outputs);
        return this;
    }

    /// <summary>
    /// Set staleness overrides.
    /// </summary>
    public OnlineQueryParamsBuilder WithStaleness(Dictionary<string, TimeSpan> staleness)
    {
        _params.Staleness ??= new();
        foreach (var (key, value) in staleness)
        {
            _params.Staleness[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Set metadata.
    /// </summary>
    public OnlineQueryParamsBuilder WithMeta(Dictionary<string, string> meta)
    {
        _params.Meta ??= new();
        foreach (var (key, value) in meta)
        {
            _params.Meta[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Add tags.
    /// </summary>
    public OnlineQueryParamsBuilder WithTags(params string[] tags)
    {
        _params.Tags ??= new();
        _params.Tags.AddRange(tags);
        return this;
    }

    /// <summary>
    /// Set whether to include metadata in response.
    /// </summary>
    public OnlineQueryParamsBuilder WithIncludeMeta(bool includeMeta = true)
    {
        _params.IncludeMeta = includeMeta;
        return this;
    }

    /// <summary>
    /// Set whether to store plan stages.
    /// </summary>
    public OnlineQueryParamsBuilder WithStorePlanStages(bool storePlanStages = true)
    {
        _params.StorePlanStages = storePlanStages;
        return this;
    }

    /// <summary>
    /// Set whether to explain the query.
    /// </summary>
    public OnlineQueryParamsBuilder WithExplain(bool explain = true)
    {
        _params.Explain = explain;
        return this;
    }

    /// <summary>
    /// Set the environment ID.
    /// </summary>
    public OnlineQueryParamsBuilder WithEnvironmentId(string environmentId)
    {
        _params.EnvironmentId = environmentId;
        return this;
    }

    /// <summary>
    /// Set the preview deployment ID.
    /// </summary>
    public OnlineQueryParamsBuilder WithPreviewDeploymentId(string previewDeploymentId)
    {
        _params.PreviewDeploymentId = previewDeploymentId;
        return this;
    }

    /// <summary>
    /// Set the query name.
    /// </summary>
    public OnlineQueryParamsBuilder WithQueryName(string queryName)
    {
        _params.QueryName = queryName;
        return this;
    }

    /// <summary>
    /// Set the query name version.
    /// </summary>
    public OnlineQueryParamsBuilder WithQueryNameVersion(string queryNameVersion)
    {
        _params.QueryNameVersion = queryNameVersion;
        return this;
    }

    /// <summary>
    /// Set the correlation ID.
    /// </summary>
    public OnlineQueryParamsBuilder WithCorrelationId(string correlationId)
    {
        _params.CorrelationId = correlationId;
        return this;
    }

    /// <summary>
    /// Set the branch.
    /// </summary>
    public OnlineQueryParamsBuilder WithBranch(string branch)
    {
        _params.Branch = branch;
        return this;
    }

    /// <summary>
    /// Set the query time.
    /// </summary>
    public OnlineQueryParamsBuilder WithNow(IEnumerable<DateTimeOffset> now)
    {
        _params.Now ??= new();
        _params.Now.AddRange(now);
        return this;
    }

    /// <summary>
    /// Set required resolver tags.
    /// </summary>
    public OnlineQueryParamsBuilder WithRequiredResolverTags(params string[] tags)
    {
        _params.RequiredResolverTags ??= new();
        _params.RequiredResolverTags.AddRange(tags);
        return this;
    }

    /// <summary>
    /// Set planner options.
    /// </summary>
    public OnlineQueryParamsBuilder WithPlannerOptions(Dictionary<string, object> options)
    {
        _params.PlannerOptions ??= new();
        foreach (var (key, value) in options)
        {
            _params.PlannerOptions[key] = value;
        }
        return this;
    }

    /// <summary>
    /// Set the timeout.
    /// </summary>
    public OnlineQueryParamsBuilder WithTimeout(TimeSpan timeout)
    {
        _params.Timeout = timeout;
        return this;
    }

    /// <summary>
    /// Build the OnlineQueryParams.
    ///
    /// When <see cref="OnlineQueryParams.QueryName"/> is set, inputs and outputs may be
    /// omitted: the server resolves them from the named query definition. Otherwise
    /// both must be provided. Note: the bulk (Feather) path still requires at least one
    /// input at request time because the Arrow record batch needs at least one column.
    /// </summary>
    public OnlineQueryParams Build()
    {
        var hasQueryName = !string.IsNullOrEmpty(_params.QueryName);
        if (_params.Inputs.Count == 0 && !hasQueryName)
        {
            throw new InvalidOperationException("At least one input is required (or set QueryName)");
        }
        if (_params.Outputs.Count == 0 && !hasQueryName)
        {
            throw new InvalidOperationException("At least one output is required (or set QueryName)");
        }
        return _params;
    }
}
