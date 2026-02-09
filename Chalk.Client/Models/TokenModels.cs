using Newtonsoft.Json;

namespace Chalk.Models;

/// <summary>
/// Request body for OAuth token exchange.
/// </summary>
internal class GetTokenRequest
{
    [JsonProperty("client_id")]
    public string ClientId { get; set; } = "";

    [JsonProperty("client_secret")]
    public string ClientSecret { get; set; } = "";

    [JsonProperty("grant_type")]
    public string GrantType { get; set; } = "client_credentials";
}

/// <summary>
/// Response from OAuth token exchange.
/// </summary>
internal class GetTokenResponse
{
    [JsonProperty("access_token")]
    public string AccessToken { get; set; } = "";

    [JsonProperty("token_type")]
    public string TokenType { get; set; } = "";

    [JsonProperty("expires_in")]
    public int ExpiresIn { get; set; }

    [JsonProperty("api_server")]
    public string? ApiServer { get; set; }

    [JsonProperty("primary_environment")]
    public string? PrimaryEnvironment { get; set; }

    [JsonProperty("expires_at")]
    public DateTimeOffset? ExpiresAt { get; set; }

    [JsonProperty("engines")]
    public Dictionary<string, string>? Engines { get; set; }
}

/// <summary>
/// Internal JWT token representation with expiry.
/// </summary>
internal class JwtToken
{
    public string Value { get; }
    public DateTimeOffset ValidUntil { get; }

    public JwtToken(string value, DateTimeOffset validUntil)
    {
        Value = value;
        ValidUntil = validUntil;
    }

    public bool IsExpired => DateTimeOffset.UtcNow.AddSeconds(10) >= ValidUntil;
}
