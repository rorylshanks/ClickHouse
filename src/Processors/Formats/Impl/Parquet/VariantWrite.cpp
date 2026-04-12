#include <Processors/Formats/Impl/Parquet/VariantWrite.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <unordered_set>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_DEEP_RECURSION;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::Parquet
{

namespace
{

struct VariantAnalyzeNode
{
    size_t object_count = 0;
    size_t array_count = 0;

    std::map<String, std::pair<size_t, DataTypePtr>, std::less<>> scalar_types;
    std::map<String, VariantAnalyzeNode, std::less<>> object_fields;
    std::unique_ptr<VariantAnalyzeNode> array_child;
};

struct VariantEncodingContext
{
    String metadata;
    std::unordered_map<String, UInt32> dictionary;
};

struct VariantTransformResult
{
    std::optional<String> residual_value;
    std::optional<Field> typed_value;
};

/// Keyed by `IDataType *` that stays alive for one `prepareVariantColumnsForWrite`
/// call. This cache is row-group local and must not outlive the encoding pass.
using VariantTransformScratch = std::unordered_map<const IDataType *, std::unordered_set<String>>;

struct VariantBuildStats
{
    std::unordered_set<String> * keys = nullptr;
    VariantAnalyzeNode * analysis = nullptr;
};

void addVariantAnalyzeScalarType(VariantAnalyzeNode & node, const DataTypePtr & type);
DataTypePtr getVariantAnalyzeScalarType(const Field & field, const DataTypePtr & type_hint);

VariantBuildStats makeVariantObjectChildStats(VariantBuildStats stats, const String & key)
{
    if (stats.keys)
        stats.keys->emplace(key);

    if (stats.analysis)
        stats.analysis = &stats.analysis->object_fields[key];

    return stats;
}

VariantBuildStats makeVariantArrayChildStats(VariantBuildStats stats)
{
    if (stats.analysis)
    {
        if (!stats.analysis->array_child)
            stats.analysis->array_child = std::make_unique<VariantAnalyzeNode>();
        stats.analysis = stats.analysis->array_child.get();
    }

    return stats;
}

template <typename BuildChild>
bool buildVariantArrayField(
    size_t size,
    VariantBuildStats stats,
    Field & out,
    BuildChild && build_child)
{
    if (stats.analysis)
    {
        ++stats.analysis->array_count;
        if (!stats.analysis->array_child)
            stats.analysis->array_child = std::make_unique<VariantAnalyzeNode>();
    }

    Array result;
    result.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        Field child;
        if (!build_child(i, makeVariantArrayChildStats(stats), child))
            return false;

        result.emplace_back(std::move(child));
    }

    out = std::move(result);
    return true;
}

template <typename BuildChild>
bool buildVariantObjectField(
    size_t size,
    VariantBuildStats stats,
    Field & out,
    BuildChild && build_child)
{
    if (stats.analysis)
        ++stats.analysis->object_count;

    Object result;
    for (size_t i = 0; i < size; ++i)
    {
        String key;
        Field child;
        if (!build_child(i, key, child))
            return false;

        if (!result.emplace(std::move(key), std::move(child)).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate key {} while preparing `Parquet` `VARIANT` object", key);
    }

    out = std::move(result);
    return true;
}

void checkVariantWriteDepth(const FormatSettings & format_settings, size_t depth)
{
    if (depth > format_settings.max_parser_depth)
    {
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while encoding `Parquet` `VARIANT`. Consider raising `max_parser_depth` setting.",
            format_settings.max_parser_depth);
    }
}

DataTypePtr getArrayChildTypeHint(const DataTypePtr & parent_type_hint, size_t index)
{
    DataTypePtr normalized_parent = unwrapVariantTypeHint(parent_type_hint);
    if (!normalized_parent)
        return nullptr;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_parent.get()))
        return array_type->getNestedType();

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_parent.get()))
    {
        if (index < tuple_type->getElements().size())
            return tuple_type->getElement(index);
    }

    return nullptr;
}

template <typename T>
bool tryConvertIntegralFieldValue(const Field & field, T & value)
{
    switch (field.getType())
    {
        case Field::Types::Int64:
        {
            Int64 source = field.safeGet<Int64>();
            if (source < static_cast<Int64>(std::numeric_limits<T>::min()) || source > static_cast<Int64>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt64:
        {
            UInt64 source = field.safeGet<UInt64>();
            if (source > static_cast<UInt64>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::Int128:
        {
            Int128 source = field.safeGet<Int128>();
            if (source < static_cast<Int128>(std::numeric_limits<T>::min()) || source > static_cast<Int128>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt128:
        {
            UInt128 source = field.safeGet<UInt128>();
            if (source > static_cast<UInt128>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::Int256:
        {
            Int256 source = field.safeGet<Int256>();
            if (source < static_cast<Int256>(std::numeric_limits<T>::min()) || source > static_cast<Int256>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt256:
        {
            UInt256 source = field.safeGet<UInt256>();
            if (source > static_cast<UInt256>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        default:
            return false;
    }
}

bool tryRescaleVariantTemporalValue(Int64 raw_value, UInt32 from_scale, UInt32 to_scale, Int64 & result)
{
    if (from_scale == to_scale)
    {
        result = raw_value;
        return true;
    }

    if (from_scale < to_scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<Int64>(to_scale - from_scale);
        return !common::mulOverflow(raw_value, multiplier, result);
    }

    result = raw_value / DecimalUtils::scaleMultiplier<Int64>(from_scale - to_scale);
    return true;
}

template <typename Predicate>
std::optional<Field> tryNormalizeVariantTemporalScalarToString(
    const Field & field,
    const DataTypePtr & type_hint,
    const FormatSettings & format_settings,
    Predicate && should_normalize)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type_hint);
    if (!normalized_type)
        return std::nullopt;

    if (!should_normalize(normalized_type))
        return std::nullopt;

    if (field.getType() == Field::Types::String)
        return field;

    auto tmp_column = normalized_type->createColumn();
    tmp_column->insert(field);

    WriteBufferFromOwnString wb;
    normalized_type->getDefaultSerialization()->serializeTextJSON(*tmp_column, 0, wb, format_settings);
    String json = wb.str();

    if (!json.empty() && json.front() == '"')
    {
        ReadBufferFromString rb(json);
        String decoded;
        readJSONString(decoded, rb, format_settings.json);
        return Field(std::move(decoded));
    }

    return Field(std::move(json));
}

DataTypePtr getObjectChildTypeHint(
    const DataTypePtr & parent_type_hint,
    const DataTypeObject * object_type,
    std::string_view child_path,
    std::string_view child_name)
{
    if (object_type)
    {
        const auto & typed_paths = object_type->getTypedPaths();
        auto it = typed_paths.find(String(child_path));
        if (it != typed_paths.end())
            return it->second;
    }

    DataTypePtr normalized_parent = unwrapVariantTypeHint(parent_type_hint);
    if (!normalized_parent)
        return nullptr;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_parent.get()))
    {
        if (!tuple_type->hasExplicitNames())
            return nullptr;

        auto position = tuple_type->tryGetPositionByName(child_name);
        if (!position.has_value())
            return nullptr;

        return tuple_type->getElement(*position);
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_parent.get()))
        return map_type->getValueType();

    return nullptr;
}

std::optional<Field> tryConvertVariantScalarToShreddedField(const Field & field, const DataTypePtr & type)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type);
    if (!normalized_type)
        return std::nullopt;

    if (isBool(normalized_type))
    {
        if (field.getType() != Field::Types::Bool)
            return std::nullopt;
        return field;
    }

    auto passthrough_if = [&](Field::Types::Which expected) -> std::optional<Field>
    {
        if (field.getType() != expected)
            return std::nullopt;
        return field;
    };

    switch (normalized_type->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Date32:
        {
            Int64 converted;
            if (!tryConvertIntegralFieldValue(field, converted))
                return std::nullopt;
            return Field(converted);
        }
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::Date:
        case TypeIndex::DateTime:
        {
            UInt64 converted;
            if (!tryConvertIntegralFieldValue(field, converted))
                return std::nullopt;
            return Field(converted);
        }
        case TypeIndex::Float32:
        case TypeIndex::Float64:
            return passthrough_if(Field::Types::Float64);
        case TypeIndex::String:
            return passthrough_if(Field::Types::String);
        case TypeIndex::UUID:
            return passthrough_if(Field::Types::UUID);
        case TypeIndex::IPv4:
            return passthrough_if(Field::Types::IPv4);
        case TypeIndex::IPv6:
            return passthrough_if(Field::Types::IPv6);
        case TypeIndex::Decimal32:
            return passthrough_if(Field::Types::Decimal32);
        case TypeIndex::Decimal64:
        case TypeIndex::DateTime64:
            return passthrough_if(Field::Types::Decimal64);
        case TypeIndex::Decimal128:
            return passthrough_if(Field::Types::Decimal128);
        case TypeIndex::Decimal256:
            return passthrough_if(Field::Types::Decimal256);
        default:
            return std::nullopt;
    }
}

bool buildVariantField(
    const Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out);

bool buildVariantFieldFromColumn(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out,
    DataTypePtr * out_value_type_hint = nullptr);

bool buildVariantField(
    const Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);

    switch (field.getType())
    {
        case Field::Types::Object:
        {
            const auto & object = field.safeGet<Object>();
            size_t index = 0;
            return buildVariantObjectField(
                object.size(),
                stats,
                out,
                [&](size_t, String & key, Field & child)
                {
                    const auto & [object_key, object_value] = *std::next(object.begin(), index++);
                    key = object_key;
                    String child_path = appendVariantJSONPath(current_path, key);
                    return buildVariantField(
                        object_value,
                        getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                        object_type,
                        child_path,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }
        case Field::Types::Array:
        {
            const auto & array = field.safeGet<Array>();
            return buildVariantArrayField(
                array.size(),
                stats,
                out,
                [&](size_t i, VariantBuildStats child_stats, Field & child)
                {
                    return buildVariantField(
                        array[i],
                        getArrayChildTypeHint(normalized_type_hint, i),
                        object_type,
                        current_path,
                        format_settings,
                        depth + 1,
                        child_stats,
                        child);
                });
        }
        case Field::Types::Tuple:
        {
            const auto & tuple = field.safeGet<Tuple>();
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type_hint.get());
            if (tuple_type && tuple_type->hasExplicitNames())
            {
                return buildVariantObjectField(
                    tuple.size(),
                    stats,
                    out,
                    [&](size_t i, String & key, Field & child)
                    {
                        key = tuple_type->getNameByPosition(i + 1);
                        String child_path = appendVariantJSONPath(current_path, key);
                        return buildVariantField(
                            tuple[i],
                            tuple_type->getElement(i),
                            object_type,
                            child_path,
                            format_settings,
                            depth + 1,
                            makeVariantObjectChildStats(stats, key),
                            child);
                    });
            }

            return buildVariantArrayField(
                tuple.size(),
                stats,
                out,
                [&](size_t i, VariantBuildStats child_stats, Field & child)
                {
                    return buildVariantField(
                        tuple[i],
                        getArrayChildTypeHint(normalized_type_hint, i),
                        object_type,
                        current_path,
                        format_settings,
                        depth + 1,
                        child_stats,
                        child);
                });
        }
        case Field::Types::Map:
        {
            const auto & map = field.safeGet<Map>();
            return buildVariantObjectField(
                map.size(),
                stats,
                out,
                [&](size_t i, String & key, Field & child)
                {
                    const auto & entry = map[i];
                    if (entry.getType() != Field::Types::Tuple)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `Map` entry type {} while preparing `Parquet` `VARIANT`", entry.getTypeName());

                    const auto & tuple = entry.safeGet<Tuple>();
                    if (tuple.size() != 2)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `Map` entry size {} while preparing `Parquet` `VARIANT`", tuple.size());

                    if (tuple[0].getType() != Field::Types::String)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");

                    key = tuple[0].safeGet<String>();
                    String child_path = appendVariantJSONPath(current_path, key);
                    return buildVariantField(
                        tuple[1],
                        getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                        object_type,
                        child_path,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }
        default:
            if (auto normalized_field = tryNormalizeVariantTemporalScalarToString(
                    field,
                    normalized_type_hint,
                    format_settings,
                    [](const DataTypePtr & normalized_type)
                    {
                        return isTime(normalized_type) || isTime64(normalized_type);
                    }))
            {
                out = std::move(*normalized_field);
                normalized_type_hint = std::make_shared<DataTypeString>();
            }
            else
            {
                out = field;
            }

            if (stats.analysis)
            {
                if (auto scalar_type = getVariantAnalyzeScalarType(out, normalized_type_hint))
                    addVariantAnalyzeScalarType(*stats.analysis, scalar_type);
            }
            return true;
    }
}

bool buildVariantFieldFromColumn(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out,
    DataTypePtr * out_value_type_hint)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type = unwrapVariantTypeHint(type);
    if (typeid_cast<const DataTypeDynamic *>(normalized_type.get()))
    {
        const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(&column);
        if (!dynamic_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `ColumnDynamic` while preparing nested `Dynamic` value for `Parquet` `VARIANT`");

        auto nested_type = dynamic_column->getTypeAt(row);
        if (!nested_type)
        {
            if (out_value_type_hint)
                out_value_type_hint->reset();
            out = Field();
            return true;
        }

        const auto & variant_column = dynamic_column->getVariantColumn();
        auto discr = variant_column.globalDiscriminatorAt(row);
        if (discr != dynamic_column->getSharedVariantDiscriminator())
        {
            const auto & nested_column = variant_column.getVariantByGlobalDiscriminator(discr);
            return buildVariantFieldFromColumn(nested_column, nested_type, variant_column.offsetAt(row), format_settings, depth + 1, stats, out, out_value_type_hint);
        }

        if (out_value_type_hint)
            *out_value_type_hint = nested_type;
        return buildVariantField(column[row], nested_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        if (tuple_type->hasExplicitNames())
        {
            return buildVariantObjectField(
                tuple_type->getElements().size(),
                stats,
                out,
                [&](size_t i, String & key, Field & child)
                {
                    key = tuple_type->getNameByPosition(i + 1);
                    return buildVariantFieldFromColumn(
                        tuple_column->getColumn(i),
                        tuple_type->getElement(i),
                        row,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }

        return buildVariantArrayField(
            tuple_type->getElements().size(),
            stats,
            out,
            [&](size_t i, VariantBuildStats child_stats, Field & child)
            {
                return buildVariantFieldFromColumn(
                    tuple_column->getColumn(i),
                    tuple_type->getElement(i),
                    row,
                    format_settings,
                    depth + 1,
                    child_stats,
                    child);
            });
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(&column);
        if (!array_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        const auto & offsets = array_column->getOffsets();
        size_t begin = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];
        return buildVariantArrayField(
            end - begin,
            stats,
            out,
            [&](size_t i, VariantBuildStats child_stats, Field & child)
            {
                return buildVariantFieldFromColumn(
                    array_column->getData(),
                    array_type->getNestedType(),
                    begin + i,
                    format_settings,
                    depth + 1,
                    child_stats,
                    child);
            });
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
    {
        const auto * map_column = typeid_cast<const ColumnMap *>(&column);
        if (!map_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        const auto & offsets = map_column->getNestedColumn().getOffsets();
        size_t begin = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];
        const auto & nested_data = map_column->getNestedData();
        const auto & keys_column = *nested_data.getColumnPtr(0);
        const auto & values_column = *nested_data.getColumnPtr(1);
        return buildVariantObjectField(
            end - begin,
            stats,
            out,
            [&](size_t i, String & key, Field & child)
            {
                Field key_field = keys_column[begin + i];
                if (key_field.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");

                key = key_field.safeGet<String>();
                return buildVariantFieldFromColumn(
                    values_column,
                    map_type->getValueType(),
                    begin + i,
                    format_settings,
                    depth + 1,
                    makeVariantObjectChildStats(stats, key),
                    child);
            });
    }

    if (out_value_type_hint)
        *out_value_type_hint = normalized_type;
    return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
}

void normalizeVariantFieldForUntypedResidual(
    Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);

    if (field.getType() == Field::Types::Object)
    {
        auto & object = field.safeGet<Object>();
        for (auto & [key, child] : object)
        {
            String child_path = appendVariantJSONPath(current_path, key);
            normalizeVariantFieldForUntypedResidual(
                child,
                getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                object_type,
                child_path,
                format_settings,
                depth + 1);
        }
        return;
    }

    if (field.getType() == Field::Types::Array)
    {
        auto & array = field.safeGet<Array>();
        for (size_t i = 0; i < array.size(); ++i)
        {
            normalizeVariantFieldForUntypedResidual(
                array[i],
                getArrayChildTypeHint(normalized_type_hint, i),
                object_type,
                current_path,
                format_settings,
                depth + 1);
        }
        return;
    }

    if (auto normalized_field = tryNormalizeVariantTemporalScalarToString(
            field,
            normalized_type_hint,
            format_settings,
            [](const DataTypePtr & normalized_type)
            {
                return isDateOrDate32(normalized_type)
                    || isDateTimeOrDateTime64(normalized_type)
                    || isTime(normalized_type)
                    || isTime64(normalized_type);
            }))
    {
        field = std::move(*normalized_field);
    }
}

void addVariantAnalyzeScalarType(VariantAnalyzeNode & node, const DataTypePtr & type)
{
    auto & [count, stored_type] = node.scalar_types[type->getName()];
    ++count;
    if (!stored_type)
        stored_type = type;
}

DataTypePtr getVariantAnalyzeScalarType(const Field & field, const DataTypePtr & type_hint)
{
    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);
    if (normalized_type_hint && tryConvertVariantScalarToShreddedField(field, normalized_type_hint))
        return normalized_type_hint;

    switch (field.getType())
    {
        case Field::Types::Null:
            return nullptr;
        case Field::Types::Bool:
            return DataTypeFactory::instance().get("Bool");
        case Field::Types::Int64:
        case Field::Types::Int128:
        case Field::Types::Int256:
            return std::make_shared<DataTypeInt64>();
        case Field::Types::UInt64:
        {
            if (field.safeGet<UInt64>() <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return std::make_shared<DataTypeInt64>();
            return std::make_shared<DataTypeFloat64>();
        }
        case Field::Types::UInt128:
        case Field::Types::UInt256:
        case Field::Types::Float64:
            return std::make_shared<DataTypeFloat64>();
        case Field::Types::String:
            return std::make_shared<DataTypeString>();
        default:
            return nullptr;
    }
}

DataTypePtr wrapVariantTypedValueType(const DataTypePtr & type)
{
    /// Nested `typed_value` lives inside the outer `{ value, typed_value }` wrapper.
    /// Structured payloads already become present/absent through that wrapper, so only
    /// scalar leaves need an inner `Nullable`.
    DataTypePtr typed_value_type = (typeid_cast<const DataTypeTuple *>(type.get()) || typeid_cast<const DataTypeArray *>(type.get()))
        ? type
        : std::make_shared<DataTypeNullable>(type);

    DataTypes elements
    {
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
        typed_value_type,
    };
    Names names {"value", "typed_value"};
    return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeTuple>(elements, names));
}

DataTypePtr constructShreddedType(const VariantAnalyzeNode & node)
{
    size_t best_count = 0;
    bool best_is_ambiguous = false;
    enum class Candidate
    {
        None,
        Scalar,
        Array,
        Object,
    };

    Candidate best = Candidate::None;
    DataTypePtr best_scalar_type;
    auto consider = [&](size_t count, Candidate candidate)
    {
        if (count == 0)
            return;

        if (count > best_count)
        {
            best_count = count;
            best = candidate;
            best_is_ambiguous = false;
        }
        else if (count == best_count)
        {
            best_is_ambiguous = true;
        }
    };

    for (const auto & [_, scalar_type] : node.scalar_types)
    {
        if (scalar_type.first > best_count)
        {
            best_count = scalar_type.first;
            best = Candidate::Scalar;
            best_scalar_type = scalar_type.second;
            best_is_ambiguous = false;
        }
        else if (scalar_type.first == best_count && best_count != 0)
        {
            best_is_ambiguous = true;
        }
    }

    consider(node.array_count, Candidate::Array);
    consider(node.object_count, Candidate::Object);

    if (best_is_ambiguous)
        return nullptr;

    switch (best)
    {
        case Candidate::Scalar:
            return best_scalar_type;
        case Candidate::Array:
        {
            if (!node.array_child)
                return nullptr;

            DataTypePtr child = constructShreddedType(*node.array_child);
            if (!child)
                return nullptr;

            return std::make_shared<DataTypeArray>(wrapVariantTypedValueType(child));
        }
        case Candidate::Object:
        {
            DataTypes field_types;
            Names field_names;

            for (const auto & [name, child_node] : node.object_fields)
            {
                DataTypePtr child = constructShreddedType(child_node);
                if (!child)
                    continue;

                field_names.push_back(name);
                field_types.push_back(wrapVariantTypedValueType(child));
            }

            if (field_types.empty())
                return nullptr;

            return std::make_shared<DataTypeTuple>(field_types, field_names);
        }
        case Candidate::None:
            return nullptr;
    }

    return nullptr;
}

template <typename T>
void writeVariantPODToBuffer(T value, char *& out)
{
    memcpy(out, &value, sizeof(T));
    out += sizeof(T);
}

void writeVariantLittleEndianToBuffer(UInt64 value, UInt8 size, char *& out)
{
    for (size_t i = 0; i < size; ++i)
    {
        *out = static_cast<char>((value >> (8 * i)) & 0xFF);
        ++out;
    }
}

size_t getVariantEncodedStringSize(std::string_view value)
{
    return value.size() <= 63 ? 1 + value.size() : 1 + sizeof(UInt32) + value.size();
}

void writeVariantStringPayloadToBuffer(std::string_view value, char *& out)
{
    if (value.size() <= 63)
    {
        UInt8 header = static_cast<UInt8>(static_cast<UInt8>(VariantBasicType::ShortString) | (static_cast<UInt8>(value.size()) << VARIANT_VALUE_HEADER_SHIFT));
        *out = static_cast<char>(header);
        ++out;
        memcpy(out, value.data(), value.size());
        out += value.size();
        return;
    }

    if (value.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` string larger than 4 GiB");

    *out = static_cast<char>(static_cast<UInt8>(VariantBasicType::Primitive) | (static_cast<UInt8>(VariantPrimitiveType::String) << VARIANT_VALUE_HEADER_SHIFT));
    ++out;
    writeVariantPODToBuffer(static_cast<UInt32>(value.size()), out);
    memcpy(out, value.data(), value.size());
    out += value.size();
}

struct VariantScalarMeasureSink
{
    size_t size = 0;

    void writePrimitiveHeader(VariantPrimitiveType)
    {
        ++size;
    }

    template <typename T>
    void writePOD(T)
    {
        size += sizeof(T);
    }

    void writeRaw(const void *, size_t raw_size)
    {
        size += raw_size;
    }

    void writeString(std::string_view value)
    {
        size += getVariantEncodedStringSize(value);
    }
};

struct VariantScalarWriteSink
{
    char *& out;

    explicit VariantScalarWriteSink(char *& out_)
        : out(out_)
    {
    }

    void writePrimitiveHeader(VariantPrimitiveType type)
    {
        *out = static_cast<char>(static_cast<UInt8>(VariantBasicType::Primitive) | (static_cast<UInt8>(type) << VARIANT_VALUE_HEADER_SHIFT));
        ++out;
    }

    template <typename T>
    void writePOD(T value)
    {
        writeVariantPODToBuffer(value, out);
    }

    void writeRaw(const void * data, size_t raw_size)
    {
        memcpy(out, data, raw_size);
        out += raw_size;
    }

    void writeString(std::string_view value)
    {
        writeVariantStringPayloadToBuffer(value, out);
    }
};

template <typename Sink, typename T>
void writeVariantSignedIntegralPrimitive(VariantPrimitiveType type, T value, Sink & sink)
{
    sink.writePrimitiveHeader(type);
    sink.writePOD(value);
}

template <typename Sink, typename T>
void writeVariantDecimalPrimitive(VariantPrimitiveType type, const DecimalField<T> & value, Sink & sink)
{
    sink.writePrimitiveHeader(type);
    sink.writePOD(static_cast<UInt8>(value.getScale()));
    sink.writePOD(value.getValue());
}

template <typename Sink>
bool tryEncodeVariantScalarUsingTypeHint(const Field & field, const DataTypePtr & type_hint, Sink & sink)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type_hint);
    if (!normalized_type)
        return false;

    switch (normalized_type->getTypeId())
    {
        case TypeIndex::Date:
        {
            UInt64 converted;
            if (!tryConvertIntegralFieldValue(field, converted) || converted > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, static_cast<Int32>(converted), sink);
            return true;
        }
        case TypeIndex::Date32:
        {
            Int32 converted;
            if (!tryConvertIntegralFieldValue(field, converted))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, converted, sink);
            return true;
        }
        case TypeIndex::DateTime:
        {
            UInt64 converted;
            if (!tryConvertIntegralFieldValue(field, converted) || converted > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(static_cast<Int64>(converted), 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `DateTime` value as `Parquet` `VARIANT` timestamp");

            const auto & date_time_type = assert_cast<const DataTypeDateTime &>(*normalized_type);
            /// `DateTime` with an explicit time zone is an instant in time, so it maps to the
            /// adjusted-to-UTC `VARIANT` timestamp. Plain `DateTime` keeps "wall clock" semantics
            /// and uses the NTZ timestamp tag instead.
            writeVariantSignedIntegralPrimitive(
                date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampMicros : VariantPrimitiveType::TimestampNtzMicros,
                micros,
                sink);
            return true;
        }
        case TypeIndex::DateTime64:
        {
            if (field.getType() != Field::Types::Decimal64)
                return false;

            const auto & decimal = field.safeGet<DecimalField<DateTime64>>();
            const auto & date_time_type = assert_cast<const DataTypeDateTime64 &>(*normalized_type);
            /// `Variant` timestamps only support microseconds and nanoseconds, so
            /// scales `0..6` map to `6` and scales `7..9` map to `9`.
            UInt32 target_scale = date_time_type.getScale() <= 6 ? 6 : 9;

            Int64 scaled = 0;
            if (!tryRescaleVariantTemporalValue(decimal.getValue().value, date_time_type.getScale(), target_scale, scaled))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `DateTime64` value as `Parquet` `VARIANT` timestamp");

            VariantPrimitiveType primitive_type;
            if (target_scale == 6)
                primitive_type = date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampMicros : VariantPrimitiveType::TimestampNtzMicros;
            else
                primitive_type = date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampNanos : VariantPrimitiveType::TimestampNtzNanos;

            writeVariantSignedIntegralPrimitive(primitive_type, scaled, sink);
            return true;
        }
        case TypeIndex::Time:
        {
            Int64 converted;
            if (!tryConvertIntegralFieldValue(field, converted))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(converted, 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Time64:
        {
            if (field.getType() != Field::Types::Decimal64)
                return false;

            const auto & decimal = field.safeGet<DecimalField<Time64>>();
            const auto & time_type = assert_cast<const DataTypeTime64 &>(*normalized_type);

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(decimal.getValue().value, time_type.getScale(), 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time64` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Int8:
        {
            Int8 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int8, value, sink);
            return true;
        }
        case TypeIndex::Int16:
        {
            Int16 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::Int32:
        {
            Int32 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt8:
        {
            Int16 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::UInt16:
        {
            Int32 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt32:
        {
            Int64 value;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, value, sink);
            return true;
        }
        case TypeIndex::Float32:
        {
            if (field.getType() != Field::Types::Float64)
                return false;

            Float64 value = field.safeGet<Float64>();
            if (!std::isfinite(value))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `FLOAT`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Float);
            sink.writePOD(static_cast<Float32>(value));
            return true;
        }
        default:
            return false;
    }
}

template <typename Sink>
void encodeVariantScalarField(const Field & field, const DataTypePtr & type_hint, Sink & sink)
{
    if (tryEncodeVariantScalarUsingTypeHint(field, type_hint, sink))
        return;

    switch (field.getType())
    {
        case Field::Types::Null:
            sink.writePrimitiveHeader(VariantPrimitiveType::Null);
            return;
        case Field::Types::Bool:
            sink.writePrimitiveHeader(field.safeGet<bool>() ? VariantPrimitiveType::BooleanTrue : VariantPrimitiveType::BooleanFalse);
            return;
        case Field::Types::Int64:
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, field.safeGet<Int64>(), sink);
            return;
        case Field::Types::UInt64:
        {
            UInt64 source = field.safeGet<UInt64>();
            if (source > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer {} as `Parquet` `VARIANT` `INT64`", source);

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, static_cast<Int64>(source), sink);
            return;
        }
        case Field::Types::Int128:
        case Field::Types::UInt128:
        case Field::Types::Int256:
        case Field::Types::UInt256:
        {
            Int64 converted;
            if (!tryConvertIntegralFieldValue(field, converted))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer value as `Parquet` `VARIANT` `INT64`");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, converted, sink);
            return;
        }
        case Field::Types::Float64:
        {
            Float64 source = field.safeGet<Float64>();
            if (!std::isfinite(source))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `DOUBLE`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Double);
            sink.writePOD(source);
            return;
        }
        case Field::Types::String:
            sink.writeString(field.safeGet<String>());
            return;
        case Field::Types::UUID:
        {
            sink.writePrimitiveHeader(VariantPrimitiveType::UUID);
            const auto & raw = field.safeGet<UUID>().toUnderType();
            sink.writeRaw(raw.items, 16);
            return;
        }
        case Field::Types::Decimal32:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal4, field.safeGet<DecimalField<Decimal32>>(), sink);
            return;
        case Field::Types::Decimal64:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal8, field.safeGet<DecimalField<Decimal64>>(), sink);
            return;
        case Field::Types::Decimal128:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal16, field.safeGet<DecimalField<Decimal128>>(), sink);
            return;
        case Field::Types::IPv4:
        case Field::Types::IPv6:
        {
            WriteBufferFromOwnString wb;
            if (field.getType() == Field::Types::IPv4)
                writeText(field.safeGet<IPv4>(), wb);
            else
                writeText(field.safeGet<IPv6>(), wb);
            String text = wb.str();
            sink.writeString(text);
            return;
        }
        case Field::Types::Decimal256:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot encode `Decimal256` as `Parquet` `VARIANT`");
        default:
            break;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported value while encoding `Parquet` `VARIANT`");
}

template <typename Sink>
void encodeVariantScalarValue(const Field & field, const DataTypePtr & type_hint, Sink & sink)
{
    encodeVariantScalarField(field, type_hint, sink);
}

size_t getVariantScalarEncodedSize(const Field & field, const DataTypePtr & type_hint)
{
    VariantScalarMeasureSink sink;
    encodeVariantScalarValue(field, type_hint, sink);
    return sink.size;
}

void writeVariantScalarToBuffer(const Field & field, const DataTypePtr & type_hint, char *& out)
{
    VariantScalarWriteSink sink(out);
    encodeVariantScalarValue(field, type_hint, sink);
}

struct MeasuredVariantObjectChild
{
    UInt32 field_id = 0;
    const Field * child = nullptr;
    DataTypePtr type_hint;
    String path;
    size_t child_size = 0;
};

struct MeasuredVariantObjectEncoding
{
    bool omitted = false;
    UInt8 field_id_size = 0;
    UInt8 field_offset_size = 0;
    bool is_large = false;
    UInt64 total_children_size = 0;
    size_t total_size = 0;
    std::vector<MeasuredVariantObjectChild> children;
};

struct MeasuredVariantArrayEncoding
{
    UInt8 field_offset_size = 0;
    bool is_large = false;
    UInt64 total_children_size = 0;
    size_t total_size = 0;
    std::vector<size_t> child_sizes;
};

size_t measureVariantValueEncodedSize(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields = nullptr);

MeasuredVariantObjectEncoding measureVariantObjectEncoding(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields = nullptr)
{
    chassert(value.getType() == Field::Types::Object);
    const auto & object = value.safeGet<Object>();

    MeasuredVariantObjectEncoding encoding;
    encoding.children.reserve(object.size());

    UInt32 highest_field_id = 0;
    for (const auto & [key, child_value] : object)
    {
        if (excluded_fields && excluded_fields->contains(key))
            continue;

        auto it = dictionary.find(key);
        if (it == dictionary.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `Parquet` `VARIANT` dictionary entry for key {}", key);

        String child_path = appendVariantJSONPath(current_path, key);
        DataTypePtr child_type_hint = getObjectChildTypeHint(type_hint, object_type, child_path, key);
        size_t child_size = measureVariantValueEncodedSize(child_value, child_type_hint, object_type, child_path, dictionary);
        highest_field_id = std::max(highest_field_id, it->second);
        encoding.total_children_size += child_size;
        encoding.children.emplace_back(MeasuredVariantObjectChild
        {
            .field_id = it->second,
            .child = &child_value,
            .type_hint = std::move(child_type_hint),
            .path = std::move(child_path),
            .child_size = child_size,
        });
    }

    if (encoding.children.empty() && excluded_fields && !object.empty())
    {
        encoding.omitted = true;
        return encoding;
    }

    encoding.field_id_size = variantByteLength(highest_field_id);
    encoding.field_offset_size = variantByteLength(encoding.total_children_size);
    if (encoding.field_id_size > 4 || encoding.field_offset_size > 4)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` object header requiring field id size {} and offset size {}; maximum supported header width is 4 bytes",
            static_cast<UInt32>(encoding.field_id_size),
            static_cast<UInt32>(encoding.field_offset_size));
    }

    if (encoding.children.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` object with more than {} fields", std::numeric_limits<UInt32>::max());

    encoding.is_large = encoding.children.size() > std::numeric_limits<UInt8>::max();
    encoding.total_size = 1
        + (encoding.is_large ? sizeof(UInt32) : sizeof(UInt8))
        + encoding.children.size() * encoding.field_id_size
        + (encoding.children.size() + 1) * encoding.field_offset_size
        + encoding.total_children_size;
    return encoding;
}

MeasuredVariantArrayEncoding measureVariantArrayEncoding(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary)
{
    chassert(value.getType() == Field::Types::Array);
    const auto & array = value.safeGet<Array>();

    MeasuredVariantArrayEncoding encoding;
    encoding.child_sizes.reserve(array.size());
    for (size_t i = 0; i < array.size(); ++i)
    {
        size_t child_size = measureVariantValueEncodedSize(array[i], getArrayChildTypeHint(type_hint, i), object_type, current_path, dictionary);
        encoding.total_children_size += child_size;
        encoding.child_sizes.emplace_back(child_size);
    }

    encoding.field_offset_size = variantByteLength(encoding.total_children_size);
    if (encoding.field_offset_size > 4)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` array header requiring offset size {}; maximum supported header width is 4 bytes",
            static_cast<UInt32>(encoding.field_offset_size));
    }

    if (encoding.child_sizes.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` array with more than {} elements", std::numeric_limits<UInt32>::max());

    encoding.is_large = encoding.child_sizes.size() > std::numeric_limits<UInt8>::max();
    encoding.total_size = 1
        + (encoding.is_large ? sizeof(UInt32) : sizeof(UInt8))
        + (encoding.child_sizes.size() + 1) * encoding.field_offset_size
        + encoding.total_children_size;
    return encoding;
}

size_t measureVariantValueEncodedSize(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields)
{
    if (value.getType() != Field::Types::Object && value.getType() != Field::Types::Array)
        return getVariantScalarEncodedSize(value, type_hint);

    if (value.getType() == Field::Types::Object)
        return measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary, excluded_fields).total_size;

    return measureVariantArrayEncoding(value, type_hint, object_type, current_path, dictionary).total_size;
}

void writeVariantEncodedValueToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    char *& out);

void writeVariantObjectToBuffer(
    const DataTypeObject * object_type,
    const std::unordered_map<String, UInt32> & dictionary,
    const MeasuredVariantObjectEncoding & encoding,
    char *& out)
{
    chassert(!encoding.omitted);

    UInt8 header = static_cast<UInt8>(VariantBasicType::Object);
    header |= static_cast<UInt8>(encoding.field_offset_size - 1) << VARIANT_VALUE_HEADER_SHIFT;
    header |= static_cast<UInt8>(encoding.field_id_size - 1) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT);
    header |= static_cast<UInt8>(encoding.is_large) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_OBJECT_IS_LARGE_SHIFT);
    *out = static_cast<char>(header);
    ++out;

    if (encoding.is_large)
        writeVariantPODToBuffer(static_cast<UInt32>(encoding.children.size()), out);
    else
    {
        *out = static_cast<char>(static_cast<UInt8>(encoding.children.size()));
        ++out;
    }

    for (const auto & child : encoding.children)
        writeVariantLittleEndianToBuffer(child.field_id, encoding.field_id_size, out);

    UInt64 offset = 0;
    for (const auto & child : encoding.children)
    {
        writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);
        offset += child.child_size;
    }
    writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);

    for (const auto & child : encoding.children)
        writeVariantEncodedValueToBuffer(*child.child, child.type_hint, object_type, child.path, dictionary, out);
}

void writeVariantArrayToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const MeasuredVariantArrayEncoding & encoding,
    char *& out)
{
    chassert(value.getType() == Field::Types::Array);
    const auto & array = value.safeGet<Array>();

    UInt8 header = static_cast<UInt8>(VariantBasicType::Array);
    header |= static_cast<UInt8>(encoding.field_offset_size - 1) << VARIANT_VALUE_HEADER_SHIFT;
    header |= static_cast<UInt8>(encoding.is_large) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_ARRAY_IS_LARGE_SHIFT);
    *out = static_cast<char>(header);
    ++out;

    if (encoding.is_large)
        writeVariantPODToBuffer(static_cast<UInt32>(encoding.child_sizes.size()), out);
    else
    {
        *out = static_cast<char>(static_cast<UInt8>(encoding.child_sizes.size()));
        ++out;
    }

    UInt64 offset = 0;
    for (size_t child_size : encoding.child_sizes)
    {
        writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);
        offset += child_size;
    }
    writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);

    for (size_t i = 0; i < array.size(); ++i)
        writeVariantEncodedValueToBuffer(array[i], getArrayChildTypeHint(type_hint, i), object_type, current_path, dictionary, out);
}

void writeVariantEncodedValueToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    char *& out)
{
    if (value.getType() != Field::Types::Object && value.getType() != Field::Types::Array)
    {
        writeVariantScalarToBuffer(value, type_hint, out);
        return;
    }

    if (value.getType() == Field::Types::Object)
    {
        auto encoding = measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary);
        writeVariantObjectToBuffer(object_type, dictionary, encoding, out);
        return;
    }

    auto encoding = measureVariantArrayEncoding(value, type_hint, object_type, current_path, dictionary);
    writeVariantArrayToBuffer(value, type_hint, object_type, current_path, dictionary, encoding, out);
}

void encodeVariantObject(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    std::optional<String> & out,
    const std::unordered_set<String> * excluded_fields = nullptr)
{
    chassert(value.getType() == Field::Types::Object);
    auto encoding = measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary, excluded_fields);
    if (encoding.omitted)
    {
        out = std::nullopt;
        return;
    }

    out = String(encoding.total_size, '\0');
    char * out_pos = out->data();
    writeVariantObjectToBuffer(object_type, dictionary, encoding, out_pos);
    chassert(out_pos == out->data() + out->size());
}

void encodeVariantValue(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    String & out)
{
    size_t total_size = measureVariantValueEncodedSize(value, type_hint, object_type, current_path, dictionary);
    out.resize(total_size);
    char * out_pos = out.data();
    writeVariantEncodedValueToBuffer(value, type_hint, object_type, current_path, dictionary, out_pos);
    chassert(out_pos == out.data() + out.size());
}

VariantEncodingContext buildVariantEncodingContext(const std::unordered_set<String> & unique_keys)
{
    std::vector<String> keys;
    keys.reserve(unique_keys.size());
    for (const auto & key : unique_keys)
        keys.emplace_back(key);

    std::sort(keys.begin(), keys.end());

    UInt64 dictionary_bytes = 0;
    for (const auto & key : keys)
        dictionary_bytes += key.size();

    if (dictionary_bytes > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The total length of the `Parquet` `VARIANT` dictionary exceeds 4 GiB");

    VariantEncodingContext context;
    context.dictionary.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        context.dictionary.emplace(keys[i], static_cast<UInt32>(i));

    UInt8 offset_size = variantByteLength(dictionary_bytes);
    UInt8 header = 1;
    header |= static_cast<UInt8>(1) << 4;
    header |= static_cast<UInt8>(offset_size - 1) << 6;
    context.metadata.push_back(static_cast<char>(header));
    appendVariantLittleEndian(keys.size(), offset_size, context.metadata);

    UInt64 offset = 0;
    for (const auto & key : keys)
    {
        appendVariantLittleEndian(offset, offset_size, context.metadata);
        offset += key.size();
    }
    appendVariantLittleEndian(offset, offset_size, context.metadata);
    for (const auto & key : keys)
        context.metadata += key;

    return context;
}

bool transformVariantElement(
    const Field & value,
    const DataTypePtr & value_type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const DataTypePtr & shredded_type,
    const VariantEncodingContext & context,
    VariantTransformScratch & scratch,
    VariantTransformResult & out);

bool transformVariantElement(
    const Field & value,
    const DataTypePtr & value_type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const DataTypePtr & shredded_type,
    const VariantEncodingContext & context,
    VariantTransformScratch & scratch,
    VariantTransformResult & out)
{
    DataTypePtr normalized_shredded_type = unwrapVariantTypeHint(shredded_type);
    auto build_wrapper_field = [&](const Field & nested_value, const DataTypePtr & nested_value_type_hint, const DataTypePtr & wrapper_type, std::string_view nested_path, Field & wrapped_field)
    {
        const IDataType & wrapper_data_type = typeid_cast<const DataTypeNullable *>(wrapper_type.get())
            ? *assert_cast<const DataTypeNullable &>(*wrapper_type).getNestedType()
            : *wrapper_type;
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(wrapper_data_type);
        auto typed_pos = tuple_type.tryGetPositionByName("typed_value").value();
        DataTypePtr typed_nested = tuple_type.getElement(typed_pos);
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(typed_nested.get()))
            typed_nested = nullable->getNestedType();

        VariantTransformResult transformed;
        if (!transformVariantElement(nested_value, nested_value_type_hint, object_type, nested_path, typed_nested, context, scratch, transformed))
            return false;

        auto value_pos = tuple_type.tryGetPositionByName("value").value();
        Tuple result(tuple_type.getElements().size());
        result[value_pos] = transformed.residual_value ? Field(*transformed.residual_value) : Field();
        result[typed_pos] = transformed.typed_value.has_value() ? *transformed.typed_value : tuple_type.getElement(typed_pos)->getDefault();
        wrapped_field = std::move(result);
        return true;
    };
    auto get_shredded_object_field_names = [&](const DataTypeTuple & tuple_type) -> const std::unordered_set<String> &
    {
        const IDataType * key = &tuple_type;
        auto it = scratch.find(key);
        if (it != scratch.end())
            return it->second;

        std::unordered_set<String> field_names;
        field_names.reserve(tuple_type.getElements().size());
        for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
            field_names.emplace(tuple_type.getNameByPosition(i + 1));

        return scratch.emplace(key, std::move(field_names)).first->second;
    };

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_shredded_type.get()))
    {
        if (value.getType() != Field::Types::Object)
        {
            String residual_value;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

            out = {
                .residual_value = std::move(residual_value),
                .typed_value = std::nullopt,
            };
            return true;
        }

        const auto & object = value.safeGet<Object>();
        Tuple object_fields;

        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const String & field_name = tuple_type->getNameByPosition(i + 1);
            auto child_it = object.find(field_name);
            if (child_it == object.end())
            {
                object_fields.emplace_back(Field());
                continue;
            }

            String child_path = appendVariantJSONPath(current_path, field_name);
            Field wrapped_field;
            if (!build_wrapper_field(
                    child_it->second,
                    getObjectChildTypeHint(value_type_hint, object_type, child_path, field_name),
                    tuple_type->getElement(i),
                    child_path,
                    wrapped_field))
            {
                return false;
            }

            object_fields.emplace_back(std::move(wrapped_field));
        }

        std::optional<String> residual_value;
        if (object.empty())
        {
            String encoded_object;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, encoded_object);
            residual_value = std::move(encoded_object);
        }
        else
        {
            encodeVariantObject(value, value_type_hint, object_type, current_path, context.dictionary, residual_value, &get_shredded_object_field_names(*tuple_type));
        }

        out = {
            .residual_value = std::move(residual_value),
            .typed_value = Field(object_fields),
        };
        return true;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_shredded_type.get()))
    {
        if (value.getType() != Field::Types::Array)
        {
            String residual_value;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

            out = {
                .residual_value = std::move(residual_value),
                .typed_value = std::nullopt,
            };
            return true;
        }

        const auto & array = value.safeGet<Array>();
        Array values;
        values.reserve(array.size());

        for (size_t i = 0; i < array.size(); ++i)
        {
            Field wrapped_field;
            if (!build_wrapper_field(
                    array[i],
                    getArrayChildTypeHint(value_type_hint, i),
                    array_type->getNestedType(),
                    current_path,
                    wrapped_field))
            {
                return false;
            }

            values.emplace_back(std::move(wrapped_field));
        }

        std::optional<String> residual_value;
        if (array.empty())
        {
            String encoded_array;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, encoded_array);
            residual_value = std::move(encoded_array);
        }

        out = {
            .residual_value = std::move(residual_value),
            .typed_value = Field(values),
        };
        return true;
    }

    if (auto typed_scalar = tryConvertVariantScalarToShreddedField(value, normalized_shredded_type))
    {
        out = {
            .residual_value = std::nullopt,
            .typed_value = std::move(*typed_scalar),
        };
        return true;
    }

    String residual_value;
    encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

    out = {
        .residual_value = std::move(residual_value),
        .typed_value = std::nullopt,
    };
    return true;
}

}

PreparedVariantColumns prepareVariantColumnsForWrite(
    const ColumnPtr & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    DataTypePtr shredded_type,
    DataTypePtr * out_shredded_type)
{
    ColumnPtr full_column_ptr = column->convertToFullColumnIfLowCardinality();
    const IColumn & full_column = *full_column_ptr;
    const auto * object_type = typeid_cast<const DataTypeObject *>(type.get());

    const size_t num_rows = full_column.size();
    const bool need_inference = !shredded_type && out_shredded_type;
    std::vector<Field> rows;
    std::vector<DataTypePtr> row_type_hints;
    rows.reserve(num_rows);
    row_type_hints.reserve(num_rows);
    std::unordered_set<String> unique_keys;
    VariantAnalyzeNode analysis;

    for (size_t row = 0; row < num_rows; ++row)
    {
        Field row_value;
        VariantBuildStats stats
        {
            .keys = &unique_keys,
            .analysis = need_inference ? &analysis : nullptr,
        };

        if (object_type)
        {
            if (object_type->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `JSON` `Object` columns can be written as `Parquet` `VARIANT`");

            const auto * object_column = typeid_cast<const ColumnObject *>(&full_column);
            if (!object_column)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `ColumnObject` while preparing `Parquet` `VARIANT` write columns");

            Field flat_row = (*object_column)[row];
            Field nested_row;
            if (!tryBuildNestedObjectField(std::move(flat_row), nested_row))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert `Object` row {} to nested JSON shape for `Parquet` `VARIANT` writing", row);

            if (!buildVariantField(nested_row, object_type->getPtr(), object_type, std::string_view{}, format_settings, 1, stats, row_value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot prepare `Object` row {} for `Parquet` `VARIANT` writing", row);

            row_type_hints.emplace_back(type);
        }
        else if (typeid_cast<const DataTypeDynamic *>(type.get()))
        {
            DataTypePtr row_type_hint;
            if (!buildVariantFieldFromColumn(full_column, type, row, format_settings, 1, stats, row_value, &row_type_hint))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot prepare `Parquet` `VARIANT` write value for row {}", row);

            row_type_hints.emplace_back(std::move(row_type_hint));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type {} while preparing `Parquet` `VARIANT` write columns", type->getName());
        }

        rows.emplace_back(std::move(row_value));
    }

    if (need_inference)
    {
        shredded_type = constructShreddedType(analysis);
        *out_shredded_type = shredded_type;
    }

    VariantEncodingContext shared_context = buildVariantEncodingContext(unique_keys);

    PreparedVariantColumns result;
    result.metadata_type = std::make_shared<DataTypeString>();

    MutableColumnPtr metadata = result.metadata_type->createColumn();
    MutableColumnPtr value;
    MutableColumnPtr typed_value;
    Field default_typed_value;

    if (shredded_type)
    {
        result.value_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        result.typed_value_type = typeid_cast<const DataTypeArray *>(shredded_type.get())
            ? shredded_type
            : std::make_shared<DataTypeNullable>(shredded_type);
        value = result.value_type->createColumn();
        typed_value = result.typed_value_type->createColumn();
        default_typed_value = result.typed_value_type->getDefault();
    }
    else
    {
        result.value_type = std::make_shared<DataTypeString>();
        value = result.value_type->createColumn();
    }

    metadata->reserve(num_rows);
    value->reserve(num_rows);
    if (typed_value)
        typed_value->reserve(num_rows);

    auto & metadata_string_column = assert_cast<ColumnString &>(*metadata);
    metadata_string_column.getChars().reserve((shared_context.metadata.size() + 1) * num_rows);
    VariantTransformScratch transform_scratch;

    for (size_t row = 0; row < rows.size(); ++row)
    {
        Field row_value = std::move(rows[row]);
        DataTypePtr row_type_hint = row_type_hints[row] ? row_type_hints[row] : type;

        metadata_string_column.insertData(shared_context.metadata.data(), shared_context.metadata.size());

        if (!shredded_type)
        {
            normalizeVariantFieldForUntypedResidual(row_value, row_type_hint, object_type, std::string_view{}, format_settings, 1);

            String encoded_value;
            encodeVariantValue(row_value, row_type_hint, object_type, std::string_view{}, shared_context.dictionary, encoded_value);
            assert_cast<ColumnString &>(*value).insertData(encoded_value.data(), encoded_value.size());
            continue;
        }

        VariantTransformResult transformed;
        if (!transformVariantElement(row_value, row_type_hint, object_type, std::string_view{}, shredded_type, shared_context, transform_scratch, transformed))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported shredded value while encoding `Parquet` `VARIANT`");

        auto & nullable_value = assert_cast<ColumnNullable &>(*value);
        auto & nested_value = assert_cast<ColumnString &>(nullable_value.getNestedColumn());
        auto & null_map = nullable_value.getNullMapData();
        if (!transformed.residual_value.has_value())
        {
            nested_value.insertDefault();
            null_map.push_back(UInt8(1));
        }
        else
        {
            nested_value.insertData(transformed.residual_value->data(), transformed.residual_value->size());
            null_map.push_back(UInt8(0));
        }
        typed_value->insert(transformed.typed_value.has_value() ? *transformed.typed_value : default_typed_value);
    }

    result.metadata_column = std::move(metadata);
    result.value_column = std::move(value);
    result.typed_value_column = std::move(typed_value);
    return result;
}

}
