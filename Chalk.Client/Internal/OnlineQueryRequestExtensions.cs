using Chalk.Models;

namespace Chalk.Internal;

/// <summary>
/// Shared helpers for assembling the online-query request payload across the
/// REST and gRPC clients and across the JSON and Feather header paths.
/// </summary>
internal static class OnlineQueryRequestExtensions
{
    /// <summary>
    /// Copy <c>query_name</c> and <c>query_name_version</c> from <paramref name="queryParams"/>
    /// into the outgoing payload when they are set. Sending these lets the server associate the
    /// request with a named query (and resolve inputs/outputs defined on that named query).
    /// </summary>
    public static void AddNamedQueryFields(this IDictionary<string, object> target, OnlineQueryParams queryParams)
    {
        if (!string.IsNullOrEmpty(queryParams.QueryName))
        {
            target["query_name"] = queryParams.QueryName;
        }

        if (!string.IsNullOrEmpty(queryParams.QueryNameVersion))
        {
            target["query_name_version"] = queryParams.QueryNameVersion;
        }
    }
}
