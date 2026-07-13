using Chalk.Models;
using NUnit.Framework;

namespace Chalk.Client.Tests;

/// <summary>
/// Tests for the shape validation performed when an input carries a <see cref="ValueSchemaHint"/>:
/// data that contradicts the hint must fail at the <c>WithInput</c> call site with a descriptive
/// error instead of mis-hinting the server.
/// </summary>
[TestFixture]
public class SchemaHintValidationTests
{
    private static readonly HasManySchema TransactionSchema = new(
        "transaction.id",
        "transaction.amount");

    private static OnlineQueryParamsBuilder Builder() => new();

    // ----------------------------------------------------------------
    // Shapes that match
    // ----------------------------------------------------------------

    [Test]
    public void EmptyRowList_Passes()
    {
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)new List<object?>(), schema: TransactionSchema));
    }

    [Test]
    public void RowsKeyedByFqn_Pass()
    {
        var rows = new List<object?>
        {
            new Dictionary<string, object?> { ["transaction.id"] = 1, ["transaction.amount"] = 10.0 },
        };
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: TransactionSchema));
    }

    [Test]
    public void RowsKeyedByShortName_Pass()
    {
        var rows = new List<object?>
        {
            new Dictionary<string, object?> { ["id"] = 1, ["amount"] = 10.0 },
        };
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: TransactionSchema));
    }

    [Test]
    public void RowOmittingHintedColumns_Passes()
    {
        // Missing hinted columns are fine -- the server null-fills them. Only extra columns err.
        var rows = new List<object?> { new Dictionary<string, object?> { ["id"] = 1 } };
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: TransactionSchema));
    }

    [Test]
    public void NullValue_Passes()
    {
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)null, schema: TransactionSchema));
    }

    [Test]
    public void ScalarColumnWithListOfObjectValue_IsNotRecursedInto()
    {
        // "transaction.points" has no nested schema, so its list-of-dictionaries value is opaque:
        // the dictionaries' keys must not be validated as if they were nested has-many columns.
        var schema = new HasManySchema("transaction.id", "transaction.points");
        var rows = new List<object?>
        {
            new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["points"] = new List<object?> { new Dictionary<string, object?> { ["a"] = 1, ["b"] = 2 } },
            },
        };
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: schema));
    }

    [Test]
    public void NestedHasManyRows_Pass()
    {
        var schema = new HasManySchema(
            "transaction.id",
            new HasManyColumn("transaction.line_items", new HasManySchema("line_item.id", "line_item.sku")));
        var rows = new List<object?>
        {
            new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["line_items"] = new List<object?>
                {
                    new Dictionary<string, object?> { ["id"] = 7, ["sku"] = "widget" },
                },
            },
        };
        Assert.DoesNotThrow(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: schema));
    }

    // ----------------------------------------------------------------
    // Shapes that differ
    // ----------------------------------------------------------------

    [Test]
    public void RowWithColumnNotInHint_Throws()
    {
        var rows = new List<object?>
        {
            new Dictionary<string, object?> { ["id"] = 1, ["category"] = "grocery" },
        };
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("'category'"));
        Assert.That(ex.Message, Does.Contain("transaction.id, transaction.amount"));
        Assert.That(ex.Message, Does.Contain("user.transactions"));
    }

    [Test]
    public void NonListValue_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", 42, schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("must be a list of rows"));
        Assert.That(ex.Message, Does.Contain("Int32"));
    }

    [Test]
    public void StringValue_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", "oops", schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("must be a list of rows"));
    }

    [Test]
    public void NonDictionaryRow_Throws()
    {
        var rows = new List<object?> { 42 };
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("row 0"));
        Assert.That(ex.Message, Does.Contain("dictionary of column values"));
    }

    [Test]
    public void RowListPassedToPerRowOverload_ThrowsWithGuidance()
    {
        // The classic overload mixup: a List<object?> of row dictionaries binds to the
        // one-value-per-query-row overload, so each row dictionary is treated as a whole
        // has-many value. The error should say how to pass the list as a single value.
        var rows = new List<object?>
        {
            new Dictionary<string, object?> { ["id"] = 1 },
        };
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", rows, schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("single dictionary row"));
        Assert.That(ex.Message, Does.Contain("(object?)rows"));
    }

    [Test]
    public void NestedRowWithColumnNotInNestedHint_Throws()
    {
        var schema = new HasManySchema(
            "transaction.id",
            new HasManyColumn("transaction.line_items", new HasManySchema("line_item.id")));
        var rows = new List<object?>
        {
            new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["line_items"] = new List<object?>
                {
                    new Dictionary<string, object?> { ["id"] = 7, ["sku"] = "widget" },
                },
            },
        };
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", (object?)rows, schema: schema))!;
        Assert.That(ex.Message, Does.Contain("'sku'"));
        Assert.That(ex.Message, Does.Contain("transaction.line_items"));
    }

    [Test]
    public void PerRowOverload_ValidatesEveryRowValue()
    {
        // In the one-value-per-query-row overload, each element is a whole has-many value;
        // a bad shape in any row (here: the second) must be caught.
        var values = new List<object?>
        {
            new List<object?>(),
            new List<object?> { new Dictionary<string, object?> { ["oops"] = 1 } },
        };
        var ex = Assert.Throws<ArgumentException>(() => Builder().WithInput(
            "user.transactions", values, schema: TransactionSchema))!;
        Assert.That(ex.Message, Does.Contain("'oops'"));
    }
}
