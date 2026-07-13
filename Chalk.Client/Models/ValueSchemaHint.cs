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
