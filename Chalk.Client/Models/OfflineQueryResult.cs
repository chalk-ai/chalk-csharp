using Newtonsoft.Json;

namespace Chalk.Models;

/// <summary>
/// Result of an offline query.
/// </summary>
public class OfflineQueryResult
{
    [JsonProperty("is_finished")]
    public bool IsFinished { get; set; }

    [JsonProperty("version")]
    public int? Version { get; set; }

    [JsonProperty("dataset_id")]
    public string? DatasetId { get; set; }

    [JsonProperty("dataset_name")]
    public string? DatasetName { get; set; }

    [JsonProperty("environment_id")]
    public string? EnvironmentId { get; set; }

    [JsonProperty("dataset_revision_id")]
    public string? DatasetRevisionId { get; set; }

    [JsonProperty("revisions")]
    public List<DatasetRevision>? Revisions { get; set; }

    [JsonProperty("errors")]
    public List<ServerError>? Errors { get; set; }
}

/// <summary>
/// A revision of a dataset produced by an offline query.
/// </summary>
public class DatasetRevision
{
    [JsonProperty("revision_id")]
    public string? RevisionId { get; set; }

    [JsonProperty("creator_id")]
    public string? CreatorId { get; set; }

    [JsonProperty("environment_id")]
    public string? EnvironmentId { get; set; }

    [JsonProperty("outputs")]
    public List<string>? Outputs { get; set; }

    [JsonProperty("status")]
    public string? Status { get; set; }

    [JsonProperty("num_partitions")]
    public int? NumPartitions { get; set; }

    [JsonProperty("dashboard_url")]
    public string? DashboardUrl { get; set; }

    [JsonProperty("dataset_name")]
    public string? DatasetName { get; set; }

    [JsonProperty("dataset_id")]
    public string? DatasetId { get; set; }

    [JsonProperty("branch")]
    public string? Branch { get; set; }

    [JsonProperty("created_at")]
    public DateTimeOffset? CreatedAt { get; set; }

    [JsonProperty("started_at")]
    public DateTimeOffset? StartedAt { get; set; }

    [JsonProperty("terminated_at")]
    public DateTimeOffset? TerminatedAt { get; set; }
}

/// <summary>
/// Result of polling an offline query status.
/// </summary>
public class OfflineQueryStatusResult
{
    [JsonProperty("report")]
    public OfflineQueryStatusReport? Report { get; set; }
}

/// <summary>
/// Status report for an offline query.
/// </summary>
public class OfflineQueryStatusReport
{
    [JsonProperty("status")]
    public string? Status { get; set; }

    [JsonProperty("operation_id")]
    public string? OperationId { get; set; }

    [JsonProperty("environment_id")]
    public string? EnvironmentId { get; set; }

    [JsonProperty("error")]
    public string? Error { get; set; }
}

/// <summary>
/// Result of requesting download URLs for an offline query.
/// </summary>
public class OfflineQueryDownloadResult
{
    [JsonProperty("is_finished")]
    public bool IsFinished { get; set; }

    [JsonProperty("urls")]
    public List<string>? Urls { get; set; }

    [JsonProperty("errors")]
    public List<ServerError>? Errors { get; set; }
}
