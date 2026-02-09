# Chalk C# Client

Official C# client library for the [Chalk](https://chalk.ai) feature platform.

## Installation

```bash
dotnet add package Chalk.Client
```

## Quick Start

```csharp
using Chalk;
using Chalk.Models;

// Create a client using environment variables or chalk.yaml
using var client = ChalkClient.Create();

// Or use the builder for custom configuration
using var client = ChalkClient.Builder()
    .WithClientId("your-client-id")
    .WithClientSecret("your-client-secret")
    .WithEnvironmentId("your-environment-id")
    .Build();

// Execute an online query
var queryParams = new OnlineQueryParamsBuilder()
    .WithInput("user.id", 123)
    .WithOutputs("user.name", "user.email", "user.credit_score")
    .Build();

var result = await client.OnlineQueryAsync(queryParams);

// Get individual values
var name = result.GetValue<string>("user.name");
var creditScore = result.GetValue<double>("user.credit_score");
```

## Configuration

The client can be configured in multiple ways, with the following precedence:

1. **Builder methods** (highest priority)
2. **Environment variables**
3. **chalk.yaml/chalk.yml file**
4. **Default values**

### Environment Variables

| Variable | Description |
|----------|-------------|
| `CHALK_CLIENT_ID` | Client ID for authentication |
| `CHALK_CLIENT_SECRET` | Client secret for authentication |
| `CHALK_ACTIVE_ENVIRONMENT` | Environment ID |
| `CHALK_API_SERVER` | API server URL (default: https://api.chalk.ai) |
| `CHALK_QUERY_SERVER` | Custom query server URL |
| `CHALK_BRANCH` | Branch name |
| `CHALK_DEPLOYMENT_TAG` | Deployment tag |

### chalk.yaml

Create a `chalk.yaml` or `chalk.yml` file in your project root or home directory (`~/.chalk.yml`):

```yaml
client_id: your-client-id
client_secret: your-client-secret
active_environment: your-environment-id
api_server: https://api.chalk.ai
```

## Bulk Queries

For multiple rows, pass lists of values:

```csharp
var queryParams = new OnlineQueryParamsBuilder()
    .WithInput("user.id", new List<object?> { 1, 2, 3 })
    .WithOutputs("user.name", "user.email")
    .Build();

var result = await client.OnlineQueryAsync(queryParams);

// Get all values for a feature
var names = result.GetValues<string>("user.name");
```

## Query Options

```csharp
var queryParams = new OnlineQueryParamsBuilder()
    .WithInput("user.id", 123)
    .WithOutputs("user.name")
    // Set staleness for features
    .WithStaleness(new Dictionary<string, TimeSpan>
    {
        ["user.name"] = TimeSpan.FromMinutes(5)
    })
    // Add metadata
    .WithMeta(new Dictionary<string, string>
    {
        ["source"] = "my-app"
    })
    // Add tags
    .WithTags("production", "v2")
    // Include execution metadata in response
    .WithIncludeMeta()
    // Set query name for tracking
    .WithQueryName("get-user-profile")
    // Set correlation ID for tracing
    .WithCorrelationId(Guid.NewGuid().ToString())
    // Set timeout
    .WithTimeout(TimeSpan.FromSeconds(30))
    .Build();
```

## Error Handling

```csharp
try
{
    var result = await client.OnlineQueryAsync(queryParams);

    // Check for server-side errors
    if (result.Errors.Count > 0)
    {
        foreach (var error in result.Errors)
        {
            Console.WriteLine($"Error: {error.Code} - {error.Message}");
        }
    }
}
catch (ClientException ex)
{
    // Configuration or serialization errors
    Console.WriteLine($"Client error: {ex.Message}");
}
catch (ServerException ex)
{
    // HTTP or API errors
    Console.WriteLine($"Server error {ex.StatusCode}: {ex.Message}");
}
```

## gRPC Client

For high-performance scenarios, you can use the gRPC client:

```csharp
using var client = ChalkClient.CreateGrpc();

// Or via builder
using var client = ChalkClient.Builder()
    .WithClientId("your-client-id")
    .WithClientSecret("your-client-secret")
    .WithGrpc()
    .Build();
```

> Note: The current gRPC implementation uses HTTP for online queries. Full gRPC protocol support is planned for a future release.

## Building from Source

### Prerequisites

- .NET 6.0 SDK or later
- (Optional) Mono for macOS testing

### Build

```bash
dotnet build
```

### Test

```bash
dotnet test
```

### Test with Mono (macOS)

```bash
brew install mono
dotnet publish -c Release
mono ./publish/Chalk.Client.Tests.dll
```

## License

Apache License 2.0
