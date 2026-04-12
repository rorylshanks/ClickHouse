#include <Processors/Formats/Impl/Parquet/VariantReader.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::Parquet::VariantReader
{

namespace
{

void ensureVariantRemaining(const UInt8 * ptr, const UInt8 * end, size_t bytes, std::string_view what)
{
    if (static_cast<size_t>(end - ptr) < bytes)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: unexpected end of {}", what);
}

UInt64 readVariantLittleEndianVariable(const UInt8 *& ptr, const UInt8 * end, size_t size, std::string_view what)
{
    ensureVariantRemaining(ptr, end, size, what);

    UInt64 value = 0;
    memcpy(&value, ptr, size);
    ptr += size;
    return value;
}

template <typename T>
T readVariantPOD(const UInt8 *& ptr, const UInt8 * end, std::string_view what)
{
    ensureVariantRemaining(ptr, end, sizeof(T), what);

    T value {};
    memcpy(&value, ptr, sizeof(value));
    ptr += sizeof(value);
    return value;
}

template <typename T>
struct VariantDecimalFieldTraits;

template <>
struct VariantDecimalFieldTraits<Int32>
{
    using FieldType = Decimal32;
};

template <>
struct VariantDecimalFieldTraits<Int64>
{
    using FieldType = Decimal64;
};

template <>
struct VariantDecimalFieldTraits<Int128>
{
    using FieldType = Decimal128;
};

template <typename T>
Field decodeVariantScaledIntegerToField(T value, UInt8 scale)
{
    using DecimalFieldType = typename VariantDecimalFieldTraits<T>::FieldType;
    return Field(DecimalField<DecimalFieldType>(static_cast<DecimalFieldType>(value), scale));
}

Field decodeValueToField(const VariantMetadata & metadata, const UInt8 *& ptr, const UInt8 * end)
{
    ensureVariantRemaining(ptr, end, 1, "`VARIANT` value header");
    UInt8 header = *ptr++;

    VariantBasicType basic_type = static_cast<VariantBasicType>(header & VARIANT_BASIC_TYPE_MASK);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    switch (basic_type)
    {
        case VariantBasicType::Primitive:
        {
            VariantPrimitiveType primitive_type = static_cast<VariantPrimitiveType>(value_header);
            switch (primitive_type)
            {
                case VariantPrimitiveType::Null:
                    return Field();
                case VariantPrimitiveType::BooleanTrue:
                    return Field(true);
                case VariantPrimitiveType::BooleanFalse:
                    return Field(false);
                case VariantPrimitiveType::Int8:
                    return Field(static_cast<Int64>(readVariantPOD<Int8>(ptr, end, "`VARIANT` `INT8`")));
                case VariantPrimitiveType::Int16:
                    return Field(static_cast<Int64>(readVariantPOD<Int16>(ptr, end, "`VARIANT` `INT16`")));
                case VariantPrimitiveType::Int32:
                    return Field(static_cast<Int64>(readVariantPOD<Int32>(ptr, end, "`VARIANT` `INT32`")));
                case VariantPrimitiveType::Int64:
                    return Field(readVariantPOD<Int64>(ptr, end, "`VARIANT` `INT64`"));
                case VariantPrimitiveType::Float:
                    return Field(static_cast<Float64>(readVariantPOD<Float32>(ptr, end, "`VARIANT` `FLOAT`")));
                case VariantPrimitiveType::Double:
                    return Field(readVariantPOD<Float64>(ptr, end, "`VARIANT` `DOUBLE`"));
                case VariantPrimitiveType::Decimal4:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL4` scale");
                    return decodeVariantScaledIntegerToField(readVariantPOD<Int32>(ptr, end, "`VARIANT` `DECIMAL4` value"), scale);
                }
                case VariantPrimitiveType::Decimal8:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL8` scale");
                    return decodeVariantScaledIntegerToField(readVariantPOD<Int64>(ptr, end, "`VARIANT` `DECIMAL8` value"), scale);
                }
                case VariantPrimitiveType::Decimal16:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL16` scale");
                    return decodeVariantScaledIntegerToField(readVariantPOD<Int128>(ptr, end, "`VARIANT` `DECIMAL16` value"), scale);
                }
                case VariantPrimitiveType::Date:
                    return Field(static_cast<Int64>(readVariantPOD<Int32>(ptr, end, "`VARIANT` `DATE`")));
                case VariantPrimitiveType::TimestampMicros:
                case VariantPrimitiveType::TimestampNtzMicros:
                case VariantPrimitiveType::TimeNtzMicros:
                case VariantPrimitiveType::TimestampNanos:
                case VariantPrimitiveType::TimestampNtzNanos:
                    return Field(readVariantPOD<Int64>(ptr, end, "`VARIANT` temporal primitive"));
                case VariantPrimitiveType::Binary:
                {
                    UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` binary size");
                    ensureVariantRemaining(ptr, end, size, "`VARIANT` binary payload");
                    String encoded = base64Encode(String(reinterpret_cast<const char *>(ptr), size));
                    ptr += size;
                    return Field(std::move(encoded));
                }
                case VariantPrimitiveType::String:
                {
                    UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` string size");
                    ensureVariantRemaining(ptr, end, size, "`VARIANT` string payload");
                    String value(reinterpret_cast<const char *>(ptr), size);
                    ptr += size;
                    return Field(std::move(value));
                }
                case VariantPrimitiveType::UUID:
                {
                    ensureVariantRemaining(ptr, end, 16, "`VARIANT` `UUID`");
                    UUID uuid;
                    memcpy(&uuid, ptr, sizeof(uuid));
                    ptr += 16;
                    return Field(uuid);
                }
            }

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` primitive tag: {}", static_cast<UInt32>(primitive_type));
        }
        case VariantBasicType::ShortString:
        {
            UInt8 size = value_header;
            ensureVariantRemaining(ptr, end, size, "`VARIANT` short string payload");
            String value(reinterpret_cast<const char *>(ptr), size);
            ptr += size;
            return Field(std::move(value));
        }
        case VariantBasicType::Object:
        {
            size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
            size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
            bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
            UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");

            const UInt8 * field_ids = ptr;
            ensureVariantRemaining(field_ids, end, num_elements * field_id_size, "`VARIANT` object field ids");
            ptr += num_elements * field_id_size;

            const UInt8 * field_offsets = ptr;
            ensureVariantRemaining(field_offsets, end, (num_elements + 1) * field_offset_size, "`VARIANT` object field offsets");
            ptr += (num_elements + 1) * field_offset_size;

            const UInt8 * values = ptr;
            const UInt8 * field_ids_ptr = field_ids;
            const UInt8 * offsets_ptr = field_offsets;
            UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + (num_elements + 1) * field_offset_size, field_offset_size, "`VARIANT` object field offset");

            if (previous_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

            Object object;
            for (UInt64 i = 0; i < num_elements; ++i)
            {
                UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids + num_elements * field_id_size, field_id_size, "`VARIANT` object field id");
                UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + (num_elements + 1) * field_offset_size, field_offset_size, "`VARIANT` object field offset");
                if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");

                const UInt8 * child_ptr = values + previous_offset;
                Field child = decodeValueToField(metadata, child_ptr, values + next_offset);
                if (child_ptr != values + next_offset)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child size");

                object[String(metadata.strings[field_id])] = std::move(child);
                previous_offset = next_offset;
            }

            ptr = values + previous_offset;
            return Field(std::move(object));
        }
        case VariantBasicType::Array:
        {
            size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
            bool is_large = ((value_header >> VARIANT_ARRAY_IS_LARGE_SHIFT) & 0x01) != 0;
            UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` array element count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` array element count");

            const UInt8 * field_offsets = ptr;
            ensureVariantRemaining(field_offsets, end, (num_elements + 1) * field_offset_size, "`VARIANT` array offsets");
            ptr += (num_elements + 1) * field_offset_size;

            const UInt8 * values = ptr;
            const UInt8 * offsets_ptr = field_offsets;
            UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + (num_elements + 1) * field_offset_size, field_offset_size, "`VARIANT` array offset");

            if (previous_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child offset");

            Array array;
            array.reserve(num_elements);
            for (UInt64 i = 0; i < num_elements; ++i)
            {
                UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + (num_elements + 1) * field_offset_size, field_offset_size, "`VARIANT` array offset");
                if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

                const UInt8 * child_ptr = values + previous_offset;
                array.emplace_back(decodeValueToField(metadata, child_ptr, values + next_offset));
                if (child_ptr != values + next_offset)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

                previous_offset = next_offset;
            }

            ptr = values + previous_offset;
            return Field(std::move(array));
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: unexpected basic type tag");
}

void writeScalarFieldAsJSON(const Field & field, WriteBuffer & out)
{
    switch (field.getType())
    {
        case Field::Types::Null:
            writeCString("null", out);
            return;
        case Field::Types::Bool:
            writeCString(field.safeGet<bool>() ? "true" : "false", out);
            return;
        case Field::Types::Int64:
            writeText(field.safeGet<Int64>(), out);
            return;
        case Field::Types::UInt64:
            writeText(field.safeGet<UInt64>(), out);
            return;
        case Field::Types::Int128:
            writeText(field.safeGet<Int128>(), out);
            return;
        case Field::Types::UInt128:
            writeText(field.safeGet<UInt128>(), out);
            return;
        case Field::Types::Int256:
            writeText(field.safeGet<Int256>(), out);
            return;
        case Field::Types::UInt256:
            writeText(field.safeGet<UInt256>(), out);
            return;
        case Field::Types::Float64:
            writeJSONNumber(field.safeGet<Float64>(), out, getDefaultJSONFormatSettings());
            return;
        case Field::Types::String:
            writeJSONString(field.safeGet<String>(), out, getDefaultJSONFormatSettings());
            return;
        case Field::Types::UUID:
        {
            WriteBufferFromOwnString wb;
            writeText(field.safeGet<UUID>(), wb);
            writeJSONString(wb.str(), out, getDefaultJSONFormatSettings());
            return;
        }
        case Field::Types::IPv4:
        {
            WriteBufferFromOwnString wb;
            writeText(field.safeGet<IPv4>(), wb);
            writeJSONString(wb.str(), out, getDefaultJSONFormatSettings());
            return;
        }
        case Field::Types::IPv6:
        {
            WriteBufferFromOwnString wb;
            writeText(field.safeGet<IPv6>(), wb);
            writeJSONString(wb.str(), out, getDefaultJSONFormatSettings());
            return;
        }
        case Field::Types::Decimal32:
            writeText(field.safeGet<DecimalField<Decimal32>>(), out);
            return;
        case Field::Types::Decimal64:
            writeText(field.safeGet<DecimalField<Decimal64>>(), out);
            return;
        case Field::Types::Decimal128:
            writeText(field.safeGet<DecimalField<Decimal128>>(), out);
            return;
        case Field::Types::Decimal256:
            writeText(field.safeGet<DecimalField<Decimal256>>(), out);
            return;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` field type {} for JSON serialization", field.getTypeName());
    }
}

void writeFieldAsJSON(const Field & field, WriteBuffer & out)
{
    if (field.getType() == Field::Types::Array)
    {
        const auto & array = field.safeGet<Array>();
        writeChar('[', out);
        for (size_t i = 0; i < array.size(); ++i)
        {
            if (i != 0)
                writeChar(',', out);
            writeFieldAsJSON(array[i], out);
        }
        writeChar(']', out);
        return;
    }

    if (field.getType() == Field::Types::Object)
    {
        const auto & object = field.safeGet<Object>();
        writeChar('{', out);
        bool first = true;
        for (const auto & [key, value] : object)
        {
            if (!first)
                writeChar(',', out);
            first = false;
            writeJSONString(key, out, getDefaultJSONFormatSettings());
            writeChar(':', out);
            writeFieldAsJSON(value, out);
        }
        writeChar('}', out);
        return;
    }

    writeScalarFieldAsJSON(field, out);
}

}

const FormatSettings & getDefaultJSONFormatSettings()
{
    static thread_local const FormatSettings settings;
    return settings;
}

const VariantMetadata & VariantMetadataCache::get(std::string_view metadata_blob)
{
    String key(metadata_blob);
    auto [it, inserted] = cache.try_emplace(key);
    if (inserted)
        it->second = decodeMetadata(std::string_view(it->first.data(), it->first.size()));
    return it->second;
}

VariantMetadata decodeMetadata(std::string_view metadata_blob)
{
    if (metadata_blob.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: empty `metadata` blob");

    const auto * ptr = reinterpret_cast<const UInt8 *>(metadata_blob.data());
    const auto * end = ptr + metadata_blob.size();

    UInt8 header = *ptr++;
    UInt8 version = header & VARIANT_METADATA_VERSION_MASK;
    if (version != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` metadata version: {}", static_cast<UInt32>(version));

    size_t offset_size = ((header >> VARIANT_METADATA_OFFSET_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    UInt64 dictionary_size = readVariantLittleEndianVariable(ptr, end, offset_size, "`VARIANT` metadata dictionary size");

    const UInt8 * offsets = ptr;
    size_t offsets_bytes = (dictionary_size + 1) * offset_size;
    ensureVariantRemaining(offsets, end, offsets_bytes, "`VARIANT` metadata offsets");
    ptr += offsets_bytes;

    const UInt8 * strings_data = ptr;
    const UInt8 * offsets_ptr = offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, offsets + offsets_bytes, offset_size, "`VARIANT` metadata offset");

    VariantMetadata metadata;
    metadata.strings_sorted = ((header >> VARIANT_METADATA_SORTED_STRINGS_SHIFT) & 0x01) != 0;
    metadata.strings.reserve(dictionary_size);
    for (UInt64 i = 0; i < dictionary_size; ++i)
    {
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, offsets + offsets_bytes, offset_size, "`VARIANT` metadata offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - strings_data))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid `metadata` string offsets");

        metadata.strings.emplace_back(reinterpret_cast<const char *>(strings_data + previous_offset), static_cast<size_t>(next_offset - previous_offset));
        previous_offset = next_offset;
    }

    if (previous_offset != static_cast<UInt64>(end - strings_data))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid `metadata` payload size");

    return metadata;
}

Field decodeToField(const VariantMetadata & metadata, std::string_view value_blob)
{
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();

    const UInt8 * consumed = begin;
    Field value = decodeValueToField(metadata, consumed, end);
    if (consumed != end)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: trailing bytes in `value` blob");

    return value;
}

void fillDecodedRowJSON(const DecodedRow & decoded_row, String & out)
{
    out.clear();
    WriteBufferFromString buffer(out);
    if (decoded_row.is_logical_null || !decoded_row.merged_field.has_value())
    {
        writeCString("null", buffer);
        return;
    }

    Field field = *decoded_row.merged_field;
    if (auto nested_field = tryNestVariantJSONPaths(std::move(field)))
        writeFieldAsJSON(*nested_field, buffer);
    else
        writeFieldAsJSON(*decoded_row.merged_field, buffer);
}

}
