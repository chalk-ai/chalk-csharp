using Newtonsoft.Json;

namespace Chalk.Models;

/// <summary>
/// Result of an upload features operation.
/// </summary>
public class UploadFeaturesResult
{
    [JsonProperty("operation_id")]
    public string? OperationId { get; set; }

    [JsonProperty("errors")]
    public List<ServerError> Errors { get; set; } = new();
}
