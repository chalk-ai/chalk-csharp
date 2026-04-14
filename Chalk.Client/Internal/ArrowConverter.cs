using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Chalk.Internal;

/// <summary>
/// Converts columnar input data to Arrow Feather (IPC file) format.
/// </summary>
internal static class ArrowConverter
{
    /// <summary>
    /// Convert columnar inputs to Feather (Arrow IPC file) bytes.
    /// </summary>
    public static byte[] InputsToFeatherBytes(Dictionary<string, IList<object?>> inputs)
    {
        if (inputs.Count == 0)
            throw new ArgumentException("Inputs must not be empty", nameof(inputs));

        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();

        foreach (var (name, values) in inputs)
        {
            var (field, array) = BuildColumn(name, values);
            fields.Add(field);
            arrays.Add(array);
        }

        var schema = new Schema(fields, null);
        var recordBatch = new RecordBatch(schema, arrays, arrays[0].Length);

        using var ms = new MemoryStream();
        using (var writer = new ArrowFileWriter(ms, schema))
        {
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }

        return ms.ToArray();
    }

    private static (Field field, IArrowArray array) BuildColumn(string name, IList<object?> values)
    {
        var arrowType = InferArrowType(values);

        return arrowType switch
        {
            Int32Type => BuildInt32Column(name, values),
            Int64Type => BuildInt64Column(name, values),
            DoubleType => BuildDoubleColumn(name, values),
            StringType => BuildStringColumn(name, values),
            BooleanType => BuildBooleanColumn(name, values),
            _ => BuildStringColumn(name, values) // fallback
        };
    }

    private static IArrowType InferArrowType(IList<object?> values)
    {
        foreach (var value in values)
        {
            if (value == null) continue;

            return value switch
            {
                int => Int64Type.Default,
                long => Int64Type.Default,
                short => Int64Type.Default,
                byte => Int64Type.Default,
                float => DoubleType.Default,
                double => DoubleType.Default,
                decimal => DoubleType.Default,
                bool => BooleanType.Default,
                string => StringType.Default,
                _ => StringType.Default
            };
        }

        // All nulls — default to string
        return StringType.Default;
    }

    private static (Field, IArrowArray) BuildInt32Column(string name, IList<object?> values)
    {
        var builder = new Int32Array.Builder();
        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(Convert.ToInt32(value));
            }
        }
        return (new Field(name, Int32Type.Default, nullable: true), builder.Build());
    }

    private static (Field, IArrowArray) BuildInt64Column(string name, IList<object?> values)
    {
        var builder = new Int64Array.Builder();
        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(Convert.ToInt64(value));
            }
        }
        return (new Field(name, Int64Type.Default, nullable: true), builder.Build());
    }

    private static (Field, IArrowArray) BuildDoubleColumn(string name, IList<object?> values)
    {
        var builder = new DoubleArray.Builder();
        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(Convert.ToDouble(value));
            }
        }
        return (new Field(name, DoubleType.Default, nullable: true), builder.Build());
    }

    private static (Field, IArrowArray) BuildStringColumn(string name, IList<object?> values)
    {
        var builder = new StringArray.Builder();
        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(value.ToString() ?? "");
            }
        }
        return (new Field(name, StringType.Default, nullable: true), builder.Build());
    }

    private static (Field, IArrowArray) BuildBooleanColumn(string name, IList<object?> values)
    {
        var builder = new BooleanArray.Builder();
        foreach (var value in values)
        {
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(Convert.ToBoolean(value));
            }
        }
        return (new Field(name, BooleanType.Default, nullable: true), builder.Build());
    }
}
