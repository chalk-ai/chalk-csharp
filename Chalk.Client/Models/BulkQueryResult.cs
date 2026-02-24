namespace Chalk.Models;

/// <summary>
/// Result of a bulk (feather) online query.
/// </summary>
public class BulkQueryResult
{
    /// <summary>
    /// Raw Feather (Arrow IPC) bytes containing the scalar result data.
    /// Can be read with Apache.Arrow's ArrowFileReader.
    /// </summary>
    public byte[]? ScalarData { get; set; }

    /// <summary>
    /// Whether the response contained data.
    /// </summary>
    public bool HasData => ScalarData != null && ScalarData.Length > 0;

    /// <summary>
    /// JSON metadata string from the response, if present.
    /// </summary>
    public string? Meta { get; set; }

    /// <summary>
    /// Errors from the response.
    /// </summary>
    public List<string> Errors { get; set; } = new();
}
