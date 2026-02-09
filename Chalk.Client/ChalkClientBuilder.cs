using Chalk.Config;
using Chalk.Exceptions;

namespace Chalk;

/// <summary>
/// Builder for creating ChalkClient instances.
/// </summary>
public class ChalkClientBuilder
{
    internal string? ClientId { get; private set; }
    internal string? ClientSecret { get; private set; }
    internal string? ApiServer { get; private set; }
    internal string? QueryServer { get; private set; }
    internal string? EnvironmentId { get; private set; }
    internal string? Branch { get; private set; }
    internal string? DeploymentTag { get; private set; }
    internal TimeSpan? Timeout { get; private set; }
    internal HttpClient? HttpClient { get; private set; }
    internal bool UseGrpc { get; private set; }

    /// <summary>
    /// Set the client ID for authentication.
    /// </summary>
    public ChalkClientBuilder WithClientId(string clientId)
    {
        ClientId = clientId;
        return this;
    }

    /// <summary>
    /// Set the client secret for authentication.
    /// </summary>
    public ChalkClientBuilder WithClientSecret(string clientSecret)
    {
        ClientSecret = clientSecret;
        return this;
    }

    /// <summary>
    /// Set the API server URL. Defaults to "https://api.chalk.ai".
    /// </summary>
    public ChalkClientBuilder WithApiServer(string apiServer)
    {
        ApiServer = apiServer;
        return this;
    }

    /// <summary>
    /// Set a custom query server URL.
    /// </summary>
    public ChalkClientBuilder WithQueryServer(string queryServer)
    {
        QueryServer = queryServer;
        return this;
    }

    /// <summary>
    /// Set the environment ID.
    /// </summary>
    public ChalkClientBuilder WithEnvironmentId(string environmentId)
    {
        EnvironmentId = environmentId;
        return this;
    }

    /// <summary>
    /// Set the branch name.
    /// </summary>
    public ChalkClientBuilder WithBranch(string branch)
    {
        Branch = branch;
        return this;
    }

    /// <summary>
    /// Set the deployment tag.
    /// </summary>
    public ChalkClientBuilder WithDeploymentTag(string deploymentTag)
    {
        DeploymentTag = deploymentTag;
        return this;
    }

    /// <summary>
    /// Set the timeout for all requests.
    /// </summary>
    public ChalkClientBuilder WithTimeout(TimeSpan timeout)
    {
        Timeout = timeout;
        return this;
    }

    /// <summary>
    /// Set a custom HttpClient.
    /// </summary>
    public ChalkClientBuilder WithHttpClient(HttpClient httpClient)
    {
        HttpClient = httpClient;
        return this;
    }

    /// <summary>
    /// Use gRPC for communication (instead of HTTP).
    /// </summary>
    public ChalkClientBuilder WithGrpc()
    {
        UseGrpc = true;
        return this;
    }

    /// <summary>
    /// Build the ChalkClient.
    /// </summary>
    public IChalkClient Build()
    {
        if (!string.IsNullOrEmpty(Branch) && !string.IsNullOrEmpty(DeploymentTag))
        {
            throw new ClientException("Cannot set both Branch and DeploymentTag");
        }

        if (UseGrpc)
        {
            return new GrpcChalkClient(this);
        }

        return new ChalkClient(this);
    }
}
