#include <Processors/Formats/Impl/Parquet/VariantReader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB::ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

namespace DB::Parquet::VariantReader
{

namespace
{

bool metadataContainsKey(const VariantMetadata & metadata, std::string_view key)
{
    if (metadata.strings_sorted)
        return std::binary_search(metadata.strings.begin(), metadata.strings.end(), key);

    return std::find(metadata.strings.begin(), metadata.strings.end(), key) != metadata.strings.end();
}

bool isAbsentTypedField(const Field & field, const DataTypePtr & type)
{
    if (field.isNull())
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        return isAbsentTypedField(field, nullable->getNestedType());

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return isAbsentTypedField(field, low_cardinality->getDictionaryType());

    if (auto wrapper = tryGetVariantWrapperLayout(type))
    {
        const auto & tuple = field.safeGet<Tuple>();

        bool has_value = !tuple.at(wrapper->value_pos).isNull();
        bool has_typed_value = false;
        if (wrapper->typed_value_pos.has_value())
        {
            has_typed_value = !isAbsentTypedField(
                tuple.at(*wrapper->typed_value_pos),
                wrapper->typed_value_type);
        }

        return !has_value && !has_typed_value;
    }

    return false;
}

std::optional<Field> tryNormalizeTemporalScalarForRead(
    const Field & field,
    const DataTypePtr & type,
    const FormatSettings & format_settings)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type);
    if (!normalized_type)
        return std::nullopt;

    if (!isDateOrDate32(normalized_type) && !isDateTimeOrDateTime64(normalized_type) && !isTime(normalized_type) && !isTime64(normalized_type))
        return std::nullopt;

    auto column = normalized_type->createColumn();
    column->insert(field);
    WriteBufferFromOwnString out;
    normalized_type->getDefaultSerialization()->serializeTextJSON(*column, 0, out, format_settings);
    String json = out.str();
    if (!json.empty() && json.front() == '"')
    {
        ReadBufferFromString rb(json);
        String decoded;
        readJSONString(decoded, rb, format_settings.json);
        return Field(std::move(decoded));
    }

    return Field(std::move(json));
}

ConvertedTypedField finalizeConvertedField(
    ConvertedTypedField result,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    if (!result.present || !result.is_empty_container || !field_name.has_value())
        return result;

    if (preserve_empty_containers_in_shared_metadata)
        return result;

    if (!metadataContainsKey(metadata, *field_name))
        return {};

    if (metadata_is_shared_across_rows)
        return {};

    return result;
}

ConvertedTypedField finalizeConvertedContainer(
    Field field,
    bool is_empty_container,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    ConvertedTypedField result;
    result.present = true;
    result.is_empty_container = is_empty_container;
    result.field = std::move(field);
    return finalizeConvertedField(std::move(result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
}

ConvertedTypedField combineConvertedFields(
    std::optional<Field> residual_field,
    ConvertedTypedField typed_result,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    ConvertedTypedField result;
    if (residual_field.has_value())
    {
        result.present = true;
        if (typed_result.present && !typed_result.is_empty_container)
            result.field = mergeFields(std::move(*residual_field), typed_result.field);
        else
            result.field = std::move(*residual_field);
    }
    else if (typed_result.present)
    {
        result.present = true;
        result.is_empty_container = typed_result.is_empty_container;
        result.field = std::move(typed_result.field);
    }

    return finalizeConvertedField(std::move(result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
}

ConvertedTypedField convertTypedFieldImpl(
    const Field & field,
    const DataTypePtr & type,
    const VariantMetadata & metadata,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    bool normalize_temporal_scalars)
{
    if (field.isNull())
        return {};

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        return convertTypedFieldImpl(field, nullable->getNestedType(), metadata, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name, normalize_temporal_scalars);
    }

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        return convertTypedFieldImpl(field, low_cardinality->getDictionaryType(), metadata, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name, normalize_temporal_scalars);
    }

    if (auto wrapper = tryGetVariantWrapperLayout(type))
    {
        const auto & tuple = field.safeGet<Tuple>();

        std::optional<Field> residual_field;
        if (!tuple.at(wrapper->value_pos).isNull())
        {
            const auto & value_blob = tuple.at(wrapper->value_pos).safeGet<String>();
            residual_field = decodeToField(metadata, std::string_view(value_blob.data(), value_blob.size()));
        }

        ConvertedTypedField typed_result;
        if (wrapper->typed_value_pos.has_value())
        {
            const auto & typed_field = tuple.at(*wrapper->typed_value_pos);
            if (!isAbsentTypedField(typed_field, wrapper->typed_value_type))
            {
                typed_result = convertTypedFieldImpl(
                    typed_field,
                    wrapper->typed_value_type,
                    metadata,
                    format_settings,
                    metadata_is_shared_across_rows,
                    preserve_empty_containers_in_shared_metadata,
                    std::nullopt,
                    normalize_temporal_scalars);
            }
        }

        return combineConvertedFields(std::move(residual_field), std::move(typed_result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple = field.safeGet<Tuple>();
        Object object;
        for (size_t i = 0; i < tuple.size(); ++i)
        {
            const auto & child_name = tuple_type->getNameByPosition(i + 1);
            auto child_result = convertTypedFieldImpl(
                tuple.at(i),
                tuple_type->getElement(i),
                metadata,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                child_name,
                normalize_temporal_scalars);
            if (!child_result.present)
                continue;

            object[child_name] = std::move(child_result.field);
        }

        bool object_is_empty = object.empty();
        return finalizeConvertedContainer(Field(std::move(object)), object_is_empty, field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & array = field.safeGet<Array>();
        Array result_array;
        result_array.reserve(array.size());
        for (const auto & element : array)
        {
            auto child_result = convertTypedFieldImpl(
                element,
                array_type->getNestedType(),
                metadata,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                std::nullopt,
                normalize_temporal_scalars);
            result_array.emplace_back(child_result.present ? std::move(child_result.field) : Field());
        }

        bool array_is_empty = result_array.empty();
        return finalizeConvertedContainer(Field(std::move(result_array)), array_is_empty, field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
    }

    ConvertedTypedField result;
    result.present = true;
    result.field = normalize_temporal_scalars
        ? tryNormalizeTemporalScalarForRead(field, type, format_settings).value_or(field)
        : field;
    return result;
}

}

Field mergeFields(Field base, const Field & patch)
{
    if (base.isNull())
        return patch;

    if (base.getType() == Field::Types::Object && patch.getType() == Field::Types::Object)
    {
        auto & base_object = base.safeGet<Object>();
        const auto & patch_object = patch.safeGet<Object>();

        for (const auto & [key, patch_value] : patch_object)
        {
            auto it = base_object.find(key);
            if (it == base_object.end())
            {
                base_object[key] = patch_value;
                continue;
            }

            it->second = mergeFields(std::move(it->second), patch_value);
        }

        return base;
    }

    if (base.getType() == Field::Types::Array && patch.getType() == Field::Types::Array)
    {
        auto & base_array = base.safeGet<Array>();
        const auto & patch_array = patch.safeGet<Array>();
        size_t original_size = base_array.size();
        if (patch_array.size() > original_size)
            base_array.resize(patch_array.size());

        for (size_t i = 0; i < patch_array.size(); ++i)
        {
            if (i < original_size)
                base_array[i] = mergeFields(std::move(base_array[i]), patch_array[i]);
            else
                base_array[i] = patch_array[i];
        }

        return base;
    }

    return patch;
}

std::vector<ConvertedTypedField> convertTypedColumnRange(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    bool normalize_temporal_scalars)
{
    auto convert_rowwise = [&]()
    {
        std::vector<ConvertedTypedField> result(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!metadata_by_row[i])
                continue;

            Field field = column[row_offset + i];
            result[i] = convertTypedFieldImpl(
                field,
                type,
                *metadata_by_row[i],
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                field_name,
                normalize_temporal_scalars);
        }
        return result;
    };

    if (num_rows == 0)
        return {};

    bool has_any_metadata = false;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (metadata_by_row[i])
        {
            has_any_metadata = true;
            break;
        }
    }

    if (!has_any_metadata)
        return std::vector<ConvertedTypedField>(num_rows);

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto * nullable_column = typeid_cast<const ColumnNullable *>(&column);
        if (!nullable_column)
            return convert_rowwise();

        auto nested_results = convertTypedColumnRange(
            nullable_column->getNestedColumn(),
            nullable->getNestedType(),
            metadata_by_row,
            num_rows,
            row_offset,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            field_name,
            normalize_temporal_scalars);

        std::vector<ConvertedTypedField> result(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!nullable_column->isNullAt(row_offset + i))
                result[i] = std::move(nested_results[i]);
        }
        return result;
    }

    if (typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return convert_rowwise();

    if (auto wrapper = tryGetVariantWrapperLayout(type))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column)
            return convert_rowwise();

        std::vector<ConvertedTypedField> typed_results;
        if (wrapper->typed_value_pos.has_value())
        {
            typed_results = convertTypedColumnRange(
                tuple_column->getColumn(*wrapper->typed_value_pos),
                wrapper->typed_value_type,
                metadata_by_row,
                num_rows,
                row_offset,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                std::nullopt,
                normalize_temporal_scalars);
        }

        std::vector<ConvertedTypedField> result(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!metadata_by_row[i])
                continue;

            const auto & metadata = *metadata_by_row[i];

            std::optional<Field> residual_field;
            const auto & value_column = tuple_column->getColumn(wrapper->value_pos);
            if (const auto * nullable = typeid_cast<const ColumnNullable *>(&value_column))
            {
                if (!nullable->isNullAt(row_offset + i))
                {
                    const auto & nested = assert_cast<const ColumnString &>(nullable->getNestedColumn());
                    auto data = nested.getDataAt(row_offset + i);
                    residual_field = decodeToField(metadata, std::string_view(data.data(), data.size()));
                }
            }
            else
            {
                const auto & string_column = assert_cast<const ColumnString &>(value_column);
                auto data = string_column.getDataAt(row_offset + i);
                residual_field = decodeToField(metadata, std::string_view(data.data(), data.size()));
            }

            ConvertedTypedField typed_result;
            if (wrapper->typed_value_pos.has_value())
                typed_result = std::move(typed_results[i]);

            result[i] = combineConvertedFields(std::move(residual_field), std::move(typed_result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column || !tuple_type->hasExplicitNames())
            return convert_rowwise();

        if (tuple_type->getElements().empty())
        {
            std::vector<ConvertedTypedField> result(num_rows);
            for (size_t row = 0; row < num_rows; ++row)
            {
                if (!metadata_by_row[row])
                    continue;

                result[row] = finalizeConvertedContainer(
                    Field(Object {}),
                    true,
                    field_name,
                    *metadata_by_row[row],
                    metadata_is_shared_across_rows,
                    preserve_empty_containers_in_shared_metadata);
            }

            return result;
        }

        std::vector<Object> objects(num_rows);
        std::vector<bool> has_values(num_rows, false);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            auto child_results = convertTypedColumnRange(
                tuple_column->getColumn(i),
                tuple_type->getElement(i),
                metadata_by_row,
                num_rows,
                row_offset,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                std::optional<std::string_view>(tuple_type->getNameByPosition(i + 1)),
                normalize_temporal_scalars);
            const auto & child_name = tuple_type->getNameByPosition(i + 1);

            for (size_t row = 0; row < num_rows; ++row)
            {
                auto & child_result = child_results[row];
                if (!child_result.present)
                    continue;

                objects[row][child_name] = std::move(child_result.field);
                has_values[row] = true;
            }
        }

        std::vector<ConvertedTypedField> result(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (!metadata_by_row[row])
                continue;

            if (!has_values[row])
                continue;

            result[row] = finalizeConvertedContainer(
                Field(std::move(objects[row])),
                false,
                field_name,
                *metadata_by_row[row],
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(&column);
        if (!array_column)
            return convert_rowwise();

        const auto & offsets = array_column->getOffsets();
        size_t begin = row_offset == 0 ? 0 : offsets[row_offset - 1];
        size_t end = offsets[row_offset + num_rows - 1];

        std::vector<const VariantMetadata *> child_metadata;
        child_metadata.reserve(end - begin);
        for (size_t row = 0; row < num_rows; ++row)
        {
            size_t element_begin = row == 0 ? begin : offsets[row_offset + row - 1];
            size_t element_end = offsets[row_offset + row];
            child_metadata.insert(child_metadata.end(), element_end - element_begin, metadata_by_row[row]);
        }

        auto child_results = convertTypedColumnRange(
            array_column->getData(),
            array_type->getNestedType(),
            child_metadata.data(),
            child_metadata.size(),
            begin,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            std::nullopt,
            normalize_temporal_scalars);

        std::vector<ConvertedTypedField> result(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (!metadata_by_row[row])
                continue;

            size_t element_begin = row == 0 ? begin : offsets[row_offset + row - 1];
            size_t element_end = offsets[row_offset + row];

            Array values;
            values.reserve(element_end - element_begin);
            for (size_t pos = element_begin; pos < element_end; ++pos)
            {
                auto & child_result = child_results[pos - begin];
                values.emplace_back(child_result.present ? std::move(child_result.field) : Field());
            }

            bool array_is_empty = values.empty();
            result[row] = finalizeConvertedContainer(
                Field(std::move(values)),
                array_is_empty,
                field_name,
                *metadata_by_row[row],
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    return convert_rowwise();
}

}
