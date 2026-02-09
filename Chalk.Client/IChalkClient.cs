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
    /// Print the current configuration.
    /// </summary>
    void PrintConfig();
}
