using System.Buffers.Binary;
using System.Text;

namespace Chalk.Internal;

/// <summary>
/// Binary protocol helpers for ByteBaseModel and Feather request formats.
/// </summary>
internal static class BinaryProtocol
{
    private static readonly byte[] ByteBaseModelMagic = Encoding.ASCII.GetBytes("CHALK_BYTE_TRANSMISSION");
    private static readonly byte[] FeatherRequestMagic = Encoding.ASCII.GetBytes("chal1");

    /// <summary>
    /// Build a ByteBaseModel binary message.
    /// Format:
    ///   Magic (22 bytes "CHALK_BYTE_TRANSMISSION")
    ///   8-byte BE length + JSON attrs
    ///   8-byte BE length + empty JSON "{}"
    ///   8-byte BE length + byte offset map JSON
    ///   Concatenated byte sections
    ///   8-byte BE length + empty JSON "{}"
    /// </summary>
    public static byte[] BuildByteBaseModel(string jsonAttrs, (string name, byte[] data)[] sections)
    {
        var emptyJson = Encoding.UTF8.GetBytes("{}");
        var attrsBytes = Encoding.UTF8.GetBytes(jsonAttrs);

        // Build offset map and concatenated sections
        var offsetMap = new Dictionary<string, long>();
        long currentOffset = 0;
        foreach (var (name, data) in sections)
        {
            offsetMap[name] = currentOffset;
            currentOffset += data.Length;
        }

        var offsetMapJson = Newtonsoft.Json.JsonConvert.SerializeObject(offsetMap);
        var offsetMapBytes = Encoding.UTF8.GetBytes(offsetMapJson);

        using var ms = new MemoryStream();

        // Magic
        ms.Write(ByteBaseModelMagic);

        // JSON attrs
        WriteLengthPrefixed(ms, attrsBytes);

        // Empty JSON
        WriteLengthPrefixed(ms, emptyJson);

        // Byte offset map
        WriteLengthPrefixed(ms, offsetMapBytes);

        // Concatenated sections
        foreach (var (_, data) in sections)
        {
            ms.Write(data);
        }

        // Trailing empty JSON
        WriteLengthPrefixed(ms, emptyJson);

        return ms.ToArray();
    }

    /// <summary>
    /// Parse a ByteBaseModel binary message.
    /// Returns the JSON attrs string and a dictionary of named byte sections.
    /// </summary>
    public static (string jsonAttrs, Dictionary<string, byte[]> sections) ParseByteBaseModel(byte[] data)
    {
        var offset = 0;

        // Verify and skip magic
        if (data.Length < ByteBaseModelMagic.Length)
            throw new InvalidOperationException("ByteBaseModel data too short for magic bytes");

        for (var i = 0; i < ByteBaseModelMagic.Length; i++)
        {
            if (data[i] != ByteBaseModelMagic[i])
                throw new InvalidOperationException("Invalid ByteBaseModel magic bytes");
        }
        offset += ByteBaseModelMagic.Length;

        // Read JSON attrs
        var attrsBytes = ReadLengthPrefixed(data, ref offset);
        var jsonAttrs = Encoding.UTF8.GetString(attrsBytes);

        // Read empty JSON (skip)
        ReadLengthPrefixed(data, ref offset);

        // Read byte offset map
        var offsetMapBytes = ReadLengthPrefixed(data, ref offset);
        var offsetMapJson = Encoding.UTF8.GetString(offsetMapBytes);
        var offsetMap = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, long>>(offsetMapJson)
                        ?? new Dictionary<string, long>();

        // Calculate total bytes section length:
        // Everything between current offset and the trailing length-prefixed empty JSON.
        // The trailing section is: 8-byte length + "{}" (2 bytes) = 10 bytes from the end.
        var trailingSectionSize = 8 + 2; // length prefix + "{}"
        var byteSectionEnd = data.Length - trailingSectionSize;
        var byteSectionStart = offset;
        var totalBytesLength = byteSectionEnd - byteSectionStart;

        // Extract sections based on offset map
        var sections = new Dictionary<string, byte[]>();
        var sortedEntries = offsetMap.OrderBy(kv => kv.Value).ToList();

        for (var i = 0; i < sortedEntries.Count; i++)
        {
            var entry = sortedEntries[i];
            var sectionStart = byteSectionStart + (int)entry.Value;
            int sectionLength;

            if (i + 1 < sortedEntries.Count)
            {
                sectionLength = (int)(sortedEntries[i + 1].Value - entry.Value);
            }
            else
            {
                sectionLength = byteSectionEnd - sectionStart;
            }

            var sectionData = new byte[sectionLength];
            Buffer.BlockCopy(data, sectionStart, sectionData, 0, sectionLength);
            sections[entry.Key] = sectionData;
        }

        return (jsonAttrs, sections);
    }

    /// <summary>
    /// Build a Feather request with the "chal1" binary protocol.
    /// Format:
    ///   Magic (5 bytes "chal1")
    ///   8-byte BE length + header JSON
    ///   8-byte BE length + feather bytes
    /// </summary>
    public static byte[] BuildFeatherRequest(string headerJson, byte[] featherBytes)
    {
        var headerBytes = Encoding.UTF8.GetBytes(headerJson);

        using var ms = new MemoryStream();

        // Magic
        ms.Write(FeatherRequestMagic);

        // Header JSON
        WriteLengthPrefixed(ms, headerBytes);

        // Feather bytes
        WriteLengthPrefixed(ms, featherBytes);

        return ms.ToArray();
    }

    private static void WriteLengthPrefixed(MemoryStream ms, byte[] data)
    {
        Span<byte> lenBuf = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(lenBuf, data.Length);
        ms.Write(lenBuf);
        ms.Write(data);
    }

    private static byte[] ReadLengthPrefixed(byte[] data, ref int offset)
    {
        if (offset + 8 > data.Length)
            throw new InvalidOperationException("ByteBaseModel data too short for length prefix");

        var length = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;

        if (offset + length > data.Length)
            throw new InvalidOperationException($"ByteBaseModel data too short for section of length {length}");

        var result = new byte[length];
        Buffer.BlockCopy(data, offset, result, 0, (int)length);
        offset += (int)length;

        return result;
    }
}
