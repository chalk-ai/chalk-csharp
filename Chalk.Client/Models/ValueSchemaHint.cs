using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Chalk.Models;

/// <summary>
/// A hint describing the intended schema of an input <em>value</em>, so the server can determine its
/// shape even when the value itself is type-ambiguous — most importantly an empty has-many, whose
/// columns can't be inferred from the (empty) data.
///
/// This is a polymorphic base: today the only kind is <see cref="HasManySchema"/>, but other value
/// schema kinds (structs, lists, ...) can be added in the future without changing call sites.
/// </summary>
public abstract class ValueSchemaHint
{
    /// <summary>
    /// Renders the wire representation used in the request's <c>input_schema_hint</c> field: a
    /// bracketed column projection of the hinted input, e.g. the schema of the input
    /// <c>user.txns</c> is rendered as <c>user.txns[txn.id,txn.amount]</c>.
    /// </summary>
    /// <param name="fqn">The fully-qualified name of the value the schema describes.</param>
    internal abstract string ToProjectionString(string fqn);

    /// <summary>
    /// Validates that a single input value has the shape this schema describes, so a hint that
    /// contradicts the data fails at the call site instead of mis-hinting the server.
    /// </summary>
    /// <param name="fqn">The fully-qualified name of the value, used in error messages.</param>
    /// <param name="value">One value of the hinted input (for one query row).</param>
    /// <exception cref="ArgumentException">The value's shape does not match the schema.</exception>
    internal abstract void ValidateValue(string fqn, object? value);
}

/// <summary>
/// Schema hint for a has-many input: the ordered set of columns present in the (possibly empty)
/// has-many. A column may itself carry a nested <see cref="ValueSchemaHint"/> (e.g. a has-many nested
/// inside a has-many), so both levels of columns can be specified.
/// </summary>
public sealed class HasManySchema : ValueSchemaHint
{
    /// <summary>The columns present in this has-many.</summary>
    public IReadOnlyList<HasManyColumn> Columns { get; }

    public HasManySchema(params HasManyColumn[] columns) => Columns = columns;

    public HasManySchema(IEnumerable<HasManyColumn> columns) => Columns = columns.ToList();

    internal override string ToProjectionString(string fqn) =>
        $"{fqn}[{string.Join(",", Columns.Select(c => c.ToProjectionString()))}]";

    /// <summary>
    /// A has-many value must be a list of dictionary rows whose keys all appear in
    /// <see cref="Columns"/> (matched by full fqn or by the name after the namespace, so both
    /// <c>{"transaction.id": 1}</c> and <c>{"id": 1}</c> rows satisfy a <c>transaction.id</c>
    /// column). Rows may omit hinted columns — that is the empty-has-many case the hint exists
    /// for — but a column absent from the hint is an error, since the server would plan without
    /// it and silently discard the data. Columns with a nested schema are validated recursively;
    /// columns without one are opaque, so a scalar column whose value happens to be a list of
    /// objects is not mistaken for a nested has-many.
    /// </summary>
    internal override void ValidateValue(string fqn, object? value)
    {
        if (value is null)
        {
            return;
        }
        if (value is IDictionary)
        {
            throw new ArgumentException(
                $"Input '{fqn}' has a {nameof(HasManySchema)} hint, so each of its values must be a list of rows, "
                + "but got a single dictionary row. If this is a single-row query, pass the whole row list as one "
                + $"value: WithInput(\"{fqn}\", (object?)rows, schema: ...).");
        }
        if (value is string || value is not IEnumerable rows)
        {
            throw new ArgumentException(
                $"Input '{fqn}' has a {nameof(HasManySchema)} hint, so each of its values must be a list of rows, "
                + $"but got {Describe(value)}.");
        }
        var rowIndex = 0;
        foreach (var row in rows)
        {
            ValidateRow(fqn, row, rowIndex);
            rowIndex++;
        }
    }

    private void ValidateRow(string fqn, object? row, int rowIndex)
    {
        if (row is not IDictionary rowDict)
        {
            throw new ArgumentException(
                $"Input '{fqn}' has a {nameof(HasManySchema)} hint, so every row must be a dictionary of column "
                + $"values, but row {rowIndex} is {Describe(row)}.");
        }
        foreach (DictionaryEntry entry in rowDict)
        {
            var key = entry.Key as string ?? entry.Key?.ToString() ?? "";
            var column = FindColumn(key);
            if (column is null)
            {
                var allowed = string.Join(", ", Columns.Select(c => c.Name));
                throw new ArgumentException(
                    $"Input '{fqn}' row {rowIndex} contains column '{key}', which is not in its schema hint "
                    + $"columns [{allowed}].");
            }
            column.Value?.ValidateValue(column.Name, entry.Value);
        }
    }

    private HasManyColumn? FindColumn(string key) =>
        Columns.FirstOrDefault(c => c.Name == key || NameWithoutNamespace(c.Name) == key);

    private static string NameWithoutNamespace(string fqn)
    {
        var lastDot = fqn.LastIndexOf('.');
        return lastDot < 0 ? fqn : fqn[(lastDot + 1)..];
    }

    private static string Describe(object? value) =>
        value is null ? "null" : $"a value of type {value.GetType().Name}";
}

/// <summary>
/// A column within a <see cref="HasManySchema"/>: a feature fqn, plus an optional nested value schema.
/// The nested schema is present when the column is itself a non-scalar value (e.g. a nested has-many);
/// scalar columns need only the fqn and convert implicitly from a plain string.
/// </summary>
public sealed class HasManyColumn
{
    /// <summary>The fully-qualified name of the column feature, e.g. <c>transaction.id</c>.</summary>
    public string Name { get; }

    /// <summary>The nested value schema for this column, or <c>null</c> for a scalar column.</summary>
    public ValueSchemaHint? Value { get; }

    public HasManyColumn(string name, ValueSchemaHint? value = null) => (Name, Value) = (name, value);

    /// <summary>A bare fqn string is a scalar column.</summary>
    public static implicit operator HasManyColumn(string name) => new(name);

    internal string ToProjectionString() => Value is null ? Name : Value.ToProjectionString(Name);
}
