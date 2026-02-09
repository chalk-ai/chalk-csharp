using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Chalk.Config;

/// <summary>
/// Environment variable names for Chalk configuration.
/// </summary>
public static class ConfigEnvVars
{
    public const string ClientId = "CHALK_CLIENT_ID";
    public const string ClientSecret = "CHALK_CLIENT_SECRET";
    public const string Environment = "CHALK_ACTIVE_ENVIRONMENT";
    public const string ApiServer = "CHALK_API_SERVER";
    public const string QueryServer = "CHALK_QUERY_SERVER";
    public const string Branch = "CHALK_BRANCH";
    public const string DeploymentTag = "CHALK_DEPLOYMENT_TAG";
}

/// <summary>
/// A configuration value with its source for debugging.
/// </summary>
public class SourcedConfig
{
    public string Value { get; }
    public string Source { get; }

    public SourcedConfig(string value, string source)
    {
        Value = value;
        Source = source;
    }

    public static SourcedConfig Missing() => new("", "missing");
    public bool IsEmpty => string.IsNullOrEmpty(Value);
}

/// <summary>
/// Chalk YAML configuration file model.
/// </summary>
internal class ChalkYamlConfig
{
    public string? ClientId { get; set; }
    public string? ClientSecret { get; set; }
    public string? ActiveEnvironment { get; set; }
    public string? ApiServer { get; set; }
}

/// <summary>
/// Configuration loader for Chalk client.
/// </summary>
public static class ConfigLoader
{
    private const string DefaultApiServer = "https://api.chalk.ai";
    private const string ChalkYamlFileName = "chalk.yaml";
    private const string ChalkYmlFileName = "chalk.yml";
    private const string HomeChalkFileName = ".chalk.yml";

    /// <summary>
    /// Load configuration from environment and files.
    /// </summary>
    internal static ChalkYamlConfig? LoadChalkYamlConfig()
    {
        // Try to find chalk.yaml or chalk.yml in current directory or parents
        var currentDir = Directory.GetCurrentDirectory();
        var configPath = FindConfigFile(currentDir);

        // Also check home directory
        if (configPath == null)
        {
            var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var homePath = Path.Combine(homeDir, HomeChalkFileName);
            if (File.Exists(homePath))
            {
                configPath = homePath;
            }
        }

        if (configPath != null)
        {
            try
            {
                var yaml = File.ReadAllText(configPath);
                var deserializer = new DeserializerBuilder()
                    .WithNamingConvention(UnderscoredNamingConvention.Instance)
                    .IgnoreUnmatchedProperties()
                    .Build();
                return deserializer.Deserialize<ChalkYamlConfig>(yaml);
            }
            catch
            {
                // Ignore errors loading config file
            }
        }

        return null;
    }

    private static string? FindConfigFile(string startDir)
    {
        var dir = new DirectoryInfo(startDir);
        while (dir != null)
        {
            var yamlPath = Path.Combine(dir.FullName, ChalkYamlFileName);
            if (File.Exists(yamlPath)) return yamlPath;

            var ymlPath = Path.Combine(dir.FullName, ChalkYmlFileName);
            if (File.Exists(ymlPath)) return ymlPath;

            dir = dir.Parent;
        }
        return null;
    }

    /// <summary>
    /// Get a configuration value with precedence: builder > env > yaml > default.
    /// </summary>
    public static SourcedConfig GetConfig(
        string? builderValue,
        string envVar,
        string? yamlValue,
        string? defaultValue = null)
    {
        if (!string.IsNullOrEmpty(builderValue))
        {
            return new SourcedConfig(builderValue, "builder");
        }

        var envValue = Environment.GetEnvironmentVariable(envVar);
        if (!string.IsNullOrEmpty(envValue))
        {
            return new SourcedConfig(envValue, $"environment variable {envVar}");
        }

        if (!string.IsNullOrEmpty(yamlValue))
        {
            return new SourcedConfig(yamlValue, "chalk.yaml");
        }

        if (!string.IsNullOrEmpty(defaultValue))
        {
            return new SourcedConfig(defaultValue, "default");
        }

        return SourcedConfig.Missing();
    }

    /// <summary>
    /// Get the default API server.
    /// </summary>
    public static string GetDefaultApiServer() => DefaultApiServer;
}
