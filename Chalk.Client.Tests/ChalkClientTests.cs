using Chalk;
using Chalk.Config;
using Chalk.Exceptions;
using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

[TestFixture]
public class ChalkClientTests
{
    [Test]
    public void Builder_WithBranchAndDeploymentTag_ThrowsException()
    {
        var builder = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://test.api.chalk.ai")
            .WithEnvironmentId("test-env")
            .WithBranch("test-branch")
            .WithDeploymentTag("test-tag");

        Assert.Throws<ClientException>(() => builder.Build());
    }

    [Test]
    public void Builder_CanChainMethods()
    {
        // Just verify that method chaining works
        var builder = ChalkClient.Builder()
            .WithClientId("test-client-id")
            .WithClientSecret("test-client-secret")
            .WithApiServer("https://test.api.chalk.ai")
            .WithEnvironmentId("test-env")
            .WithTimeout(TimeSpan.FromSeconds(30))
            .WithQueryServer("https://query.chalk.ai")
            .WithBranch("test-branch");

        Assert.That(builder, Is.Not.Null);
    }

    [Test]
    public void Builder_WithGrpc_CanBeBuiltWithCredentials()
    {
        // Just verify that gRPC builder can be configured
        var builder = ChalkClient.Builder()
            .WithGrpc();

        Assert.That(builder, Is.Not.Null);
    }
}

[TestFixture]
public class OnlineQueryParamsTests
{
    [Test]
    public void Builder_WithInputAndOutput_BuildsParams()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithOutputs("user.name", "user.email")
            .Build();

        Assert.That(queryParams.Inputs, Has.Count.EqualTo(1));
        Assert.That(queryParams.Inputs["user.id"], Has.Count.EqualTo(3));
        Assert.That(queryParams.Outputs, Has.Count.EqualTo(2));
        Assert.That(queryParams.Outputs, Contains.Item("user.name"));
        Assert.That(queryParams.Outputs, Contains.Item("user.email"));
    }

    [Test]
    public void Builder_SingleRowInput_BuildsParams()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 123)
            .WithOutputs("user.name")
            .Build();

        Assert.That(queryParams.Inputs["user.id"], Has.Count.EqualTo(1));
        Assert.That(queryParams.Inputs["user.id"][0], Is.EqualTo(123));
    }

    [Test]
    public void Builder_WithAllOptions_BuildsParams()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithStaleness(new Dictionary<string, TimeSpan> { ["user.name"] = TimeSpan.FromMinutes(5) })
            .WithMeta(new Dictionary<string, string> { ["source"] = "test" })
            .WithTags("tag1", "tag2")
            .WithIncludeMeta()
            .WithExplain()
            .WithQueryName("test-query")
            .WithCorrelationId("corr-123")
            .WithTimeout(TimeSpan.FromSeconds(10))
            .Build();

        Assert.That(queryParams.Staleness, Is.Not.Null);
        Assert.That(queryParams.Meta, Is.Not.Null);
        Assert.That(queryParams.Tags, Has.Count.EqualTo(2));
        Assert.That(queryParams.IncludeMeta, Is.True);
        Assert.That(queryParams.Explain, Is.True);
        Assert.That(queryParams.QueryName, Is.EqualTo("test-query"));
        Assert.That(queryParams.CorrelationId, Is.EqualTo("corr-123"));
        Assert.That(queryParams.Timeout, Is.EqualTo(TimeSpan.FromSeconds(10)));
    }

    [Test]
    public void Builder_NoInputs_ThrowsException()
    {
        var builder = new OnlineQueryParamsBuilder()
            .WithOutputs("user.name");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Test]
    public void Builder_NoOutputs_ThrowsException()
    {
        var builder = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Test]
    public void Builder_MultipleInputs_BuildsParams()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2, 3 })
            .WithInput("user.email", new List<object?> { "a@b.com", "c@d.com", "e@f.com" })
            .WithOutputs("user.name")
            .Build();

        Assert.That(queryParams.Inputs, Has.Count.EqualTo(2));
        Assert.That(queryParams.Inputs["user.id"], Has.Count.EqualTo(3));
        Assert.That(queryParams.Inputs["user.email"], Has.Count.EqualTo(3));
    }

    [Test]
    public void Builder_WithInputsDict_BuildsParams()
    {
        var inputs = new Dictionary<string, IList<object?>>
        {
            ["user.id"] = new List<object?> { 1, 2 },
            ["user.type"] = new List<object?> { "admin", "user" }
        };

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInputs(inputs)
            .WithOutputs("user.name")
            .Build();

        Assert.That(queryParams.Inputs, Has.Count.EqualTo(2));
    }

    [Test]
    public void Builder_WithOutputsList_BuildsParams()
    {
        var outputs = new List<string> { "user.name", "user.email", "user.age" };

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs(outputs)
            .Build();

        Assert.That(queryParams.Outputs, Has.Count.EqualTo(3));
    }

    [Test]
    public void Builder_WithEnvironmentId_SetsValue()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithEnvironmentId("prod-env")
            .Build();

        Assert.That(queryParams.EnvironmentId, Is.EqualTo("prod-env"));
    }

    [Test]
    public void Builder_WithBranch_SetsValue()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithBranch("feature-branch")
            .Build();

        Assert.That(queryParams.Branch, Is.EqualTo("feature-branch"));
    }

    [Test]
    public void Builder_WithPreviewDeploymentId_SetsValue()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithPreviewDeploymentId("preview-123")
            .Build();

        Assert.That(queryParams.PreviewDeploymentId, Is.EqualTo("preview-123"));
    }

    [Test]
    public void Builder_WithNow_SetsValues()
    {
        var now = new List<DateTimeOffset>
        {
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddDays(-1)
        };

        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", new List<object?> { 1, 2 })
            .WithOutputs("user.name")
            .WithNow(now)
            .Build();

        Assert.That(queryParams.Now, Has.Count.EqualTo(2));
    }

    [Test]
    public void Builder_WithRequiredResolverTags_SetsValues()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithRequiredResolverTags("fast", "cached")
            .Build();

        Assert.That(queryParams.RequiredResolverTags, Has.Count.EqualTo(2));
        Assert.That(queryParams.RequiredResolverTags, Contains.Item("fast"));
    }

    [Test]
    public void Builder_WithPlannerOptions_SetsValues()
    {
        var queryParams = new OnlineQueryParamsBuilder()
            .WithInput("user.id", 1)
            .WithOutputs("user.name")
            .WithPlannerOptions(new Dictionary<string, object>
            {
                ["option1"] = true,
                ["option2"] = "value"
            })
            .Build();

        Assert.That(queryParams.PlannerOptions, Has.Count.EqualTo(2));
    }
}

[TestFixture]
public class OnlineQueryResultTests
{
    [Test]
    public void GetValue_ReturnsTypedValue()
    {
        var result = new OnlineQueryResult
        {
            Data = new Dictionary<string, List<object?>>
            {
                ["user.name"] = new List<object?> { "John Doe" },
                ["user.age"] = new List<object?> { 30L }
            }
        };

        Assert.That(result.GetValue<string>("user.name"), Is.EqualTo("John Doe"));
        Assert.That(result.GetValue<long>("user.age"), Is.EqualTo(30L));
    }

    [Test]
    public void GetValue_NullValue_ReturnsDefault()
    {
        var result = new OnlineQueryResult
        {
            Data = new Dictionary<string, List<object?>>
            {
                ["user.name"] = new List<object?> { null }
            }
        };

        Assert.That(result.GetValue<string>("user.name"), Is.Null);
    }

    [Test]
    public void GetValue_MissingFeature_ReturnsDefault()
    {
        var result = new OnlineQueryResult();

        Assert.That(result.GetValue<string>("user.name"), Is.Null);
    }

    [Test]
    public void GetValues_ReturnsAllValues()
    {
        var result = new OnlineQueryResult
        {
            Data = new Dictionary<string, List<object?>>
            {
                ["user.id"] = new List<object?> { 1L, 2L, 3L }
            }
        };

        var values = result.GetValues<long>("user.id");
        Assert.That(values, Has.Count.EqualTo(3));
        Assert.That(values, Is.EquivalentTo(new[] { 1L, 2L, 3L }));
    }

    [Test]
    public void GetValues_EmptyData_ReturnsEmptyList()
    {
        var result = new OnlineQueryResult();

        var values = result.GetValues<string>("user.name");
        Assert.That(values, Is.Empty);
    }

    [Test]
    public void Errors_DefaultsToEmptyList()
    {
        var result = new OnlineQueryResult();

        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public void Data_DefaultsToEmptyDictionary()
    {
        var result = new OnlineQueryResult();

        Assert.That(result.Data, Is.Empty);
    }

    [Test]
    public void Meta_CanBeNull()
    {
        var result = new OnlineQueryResult();

        Assert.That(result.Meta, Is.Null);
    }

    [Test]
    public void Meta_CanBeSet()
    {
        var result = new OnlineQueryResult
        {
            Meta = new QueryMeta
            {
                QueryId = "query-123",
                ExecutionDurationS = 0.5
            }
        };

        Assert.That(result.Meta, Is.Not.Null);
        Assert.That(result.Meta.QueryId, Is.EqualTo("query-123"));
        Assert.That(result.Meta.ExecutionDurationS, Is.EqualTo(0.5));
    }
}

[TestFixture]
public class ServerErrorTests
{
    [Test]
    public void ToString_FormatsCorrectly()
    {
        var error = new ServerError
        {
            Code = "RESOLVER_FAILED",
            Message = "Failed to resolve feature",
            Feature = "user.name",
            Resolver = "get_user_name"
        };

        var str = error.ToString();
        Assert.That(str, Contains.Substring("RESOLVER_FAILED"));
        Assert.That(str, Contains.Substring("Failed to resolve feature"));
        Assert.That(str, Contains.Substring("user.name"));
        Assert.That(str, Contains.Substring("get_user_name"));
    }
}

[TestFixture]
public class ConfigTests
{
    [Test]
    public void SourcedConfig_Missing_ReturnsEmptyValue()
    {
        var config = SourcedConfig.Missing();

        Assert.That(config.Value, Is.Empty);
        Assert.That(config.Source, Is.EqualTo("missing"));
        Assert.That(config.IsEmpty, Is.True);
    }

    [Test]
    public void SourcedConfig_WithValue_NotEmpty()
    {
        var config = new SourcedConfig("value", "source");

        Assert.That(config.Value, Is.EqualTo("value"));
        Assert.That(config.Source, Is.EqualTo("source"));
        Assert.That(config.IsEmpty, Is.False);
    }

    [Test]
    public void GetConfig_BuilderValue_TakesPrecedence()
    {
        var config = ConfigLoader.GetConfig(
            "builder-value",
            "NON_EXISTENT_ENV_VAR_12345",
            "yaml-value",
            "default-value");

        Assert.That(config.Value, Is.EqualTo("builder-value"));
        Assert.That(config.Source, Is.EqualTo("builder"));
    }

    [Test]
    public void GetConfig_NoBuilderValue_UsesEnvVar()
    {
        var envVarName = $"CHALK_TEST_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(envVarName, "env-value");

        try
        {
            var config = ConfigLoader.GetConfig(
                null,
                envVarName,
                "yaml-value",
                "default-value");

            Assert.That(config.Value, Is.EqualTo("env-value"));
            Assert.That(config.Source, Contains.Substring("environment variable"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(envVarName, null);
        }
    }

    [Test]
    public void GetConfig_NoBuilderOrEnv_UsesYaml()
    {
        var config = ConfigLoader.GetConfig(
            null,
            "NON_EXISTENT_ENV_VAR_12345",
            "yaml-value",
            "default-value");

        Assert.That(config.Value, Is.EqualTo("yaml-value"));
        Assert.That(config.Source, Is.EqualTo("chalk.yaml"));
    }

    [Test]
    public void GetConfig_NoOtherValues_UsesDefault()
    {
        var config = ConfigLoader.GetConfig(
            null,
            "NON_EXISTENT_ENV_VAR_12345",
            null,
            "default-value");

        Assert.That(config.Value, Is.EqualTo("default-value"));
        Assert.That(config.Source, Is.EqualTo("default"));
    }

    [Test]
    public void GetConfig_NoValues_ReturnsMissing()
    {
        var config = ConfigLoader.GetConfig(
            null,
            "NON_EXISTENT_ENV_VAR_12345",
            null,
            null);

        Assert.That(config.IsEmpty, Is.True);
        Assert.That(config.Source, Is.EqualTo("missing"));
    }

    [Test]
    public void GetDefaultApiServer_ReturnsChalkApi()
    {
        var server = ConfigLoader.GetDefaultApiServer();

        Assert.That(server, Is.EqualTo("https://api.chalk.ai"));
    }
}

[TestFixture]
public class ExceptionTests
{
    [Test]
    public void ClientException_WithMessage_SetsMessage()
    {
        var ex = new ClientException("test message");

        Assert.That(ex.Message, Is.EqualTo("test message"));
    }

    [Test]
    public void ClientException_WithInnerException_SetsInnerException()
    {
        var inner = new Exception("inner");
        var ex = new ClientException("outer", inner);

        Assert.That(ex.Message, Is.EqualTo("outer"));
        Assert.That(ex.InnerException, Is.SameAs(inner));
    }

    [Test]
    public void ServerException_WithStatusCode_SetsProperties()
    {
        var ex = new ServerException(404, "Not found");

        Assert.That(ex.StatusCode, Is.EqualTo(404));
        Assert.That(ex.Message, Is.EqualTo("Not found"));
    }

    [Test]
    public void ServerException_WithErrorDetails_SetsProperties()
    {
        var ex = new ServerException(500, "INTERNAL_ERROR", "Something went wrong");

        Assert.That(ex.StatusCode, Is.EqualTo(500));
        Assert.That(ex.ErrorCode, Is.EqualTo("INTERNAL_ERROR"));
        Assert.That(ex.ErrorMessage, Is.EqualTo("Something went wrong"));
    }
}
