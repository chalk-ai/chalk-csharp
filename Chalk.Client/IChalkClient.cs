using Chalk.Models;

namespace Chalk;

/// <summary>
/// Interface for Chalk client.
/// </summary>
public interface IChalkClient : IDisposable
{
    /// <summary>
    /// Execute an online query to compute feature values.
    /// </summary>
    /// <param name="queryParams">The query parameters</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The query result</returns>
    Task<OnlineQueryResult> OnlineQueryAsync(OnlineQueryParams queryParams, CancellationToken cancellationToken = default);

    /// <summary>
    /// Execute an online query to compute feature values (synchronous).
    /// </summary>
    /// <param name="queryParams">The query parameters</param>
    /// <returns>The query result</returns>
    OnlineQueryResult OnlineQuery(OnlineQueryParams queryParams);

    /// <summary>
    /// Execute an offline query.
    /// </summary>
    Task<OfflineQueryResult> OfflineQueryAsync(OfflineQueryParams queryParams, CancellationToken cancellationToken = default);

    /// <summary>
    /// Execute an offline query (synchronous).
    /// </summary>
    OfflineQueryResult OfflineQuery(OfflineQueryParams queryParams);

    /// <summary>
    /// Get the status of an offline query by revision ID.
    /// </summary>
    Task<OfflineQueryStatusResult> GetOfflineQueryStatusAsync(string revisionId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Wait for an offline query to complete, polling until the status is terminal.
    /// </summary>
    Task<OfflineQueryResult> WaitForOfflineQueryAsync(OfflineQueryResult result, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get download URLs for a completed offline query, polling until finished.
    /// </summary>
    Task<List<string>> GetOfflineQueryDownloadUrlsAsync(OfflineQueryResult result, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Upload feature values.
    /// </summary>
    Task<UploadFeaturesResult> UploadFeaturesAsync(Dictionary<string, IList<object?>> inputs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Upload feature values (synchronous).
    /// </summary>
    UploadFeaturesResult UploadFeatures(Dictionary<string, IList<object?>> inputs);

    /// <summary>
    /// Execute a bulk online query using the binary Feather protocol.
    /// </summary>
    Task<BulkQueryResult> OnlineQueryBulkAsync(OnlineQueryParams queryParams, CancellationToken cancellationToken = default);

    /// <summary>
    /// Execute a bulk online query using the binary Feather protocol (synchronous).
    /// </summary>
    BulkQueryResult OnlineQueryBulk(OnlineQueryParams queryParams);

    /// <summary>
    /// Print the current configuration.
    /// </summary>
    void PrintConfig();
}
