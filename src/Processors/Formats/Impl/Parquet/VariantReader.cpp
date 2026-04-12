#include <Processors/Formats/Impl/Parquet/VariantReader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/castColumn.h>
#include <IO/ReadBufferFromString.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace DB::Parquet::VariantReader
{

namespace
{

enum class OutputMode
{
    NullableString,
    Generic,
    Dynamic,
    Object,
};

std::optional<std::string_view> getNullableStringValue(const IColumn & column, size_t row_num)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row_num))
            return std::nullopt;

        const auto & nested = assert_cast<const ColumnString &>(nullable->getNestedColumn());
        auto data = nested.getDataAt(row_num);
        return std::string_view(data.data(), data.size());
    }

    const auto & string_column = assert_cast<const ColumnString &>(column);
    auto data = string_column.getDataAt(row_num);
    return std::string_view(data.data(), data.size());
}

bool fieldContainsEmptyContainerHint(const Field & field)
{
    if (field.getType() == Field::Types::Object)
    {
        const auto & object = field.safeGet<Object>();
        if (object.empty())
            return true;

        for (const auto & [_, child] : object)
        {
            if (fieldContainsEmptyContainerHint(child))
                return true;
        }

        return false;
    }

    if (field.getType() == Field::Types::Array)
    {
        const auto & array = field.safeGet<Array>();
        if (array.empty())
            return true;

        for (const auto & child : array)
        {
            if (fieldContainsEmptyContainerHint(child))
                return true;
        }

        return false;
    }

    return false;
}

Field restoreTypedEmptyContainers(Field merged_field, const Field & typed_field)
{
    if (!fieldContainsEmptyContainerHint(typed_field))
        return merged_field;

    if (typed_field.getType() == Field::Types::Object)
    {
        const auto & typed_object = typed_field.safeGet<Object>();
        if (merged_field.getType() == Field::Types::String && typed_object.empty() && merged_field.safeGet<String>() == "{}")
            return typed_field;

        if (merged_field.getType() != Field::Types::Object)
            return merged_field;

        auto & merged_object = merged_field.safeGet<Object>();
        for (const auto & [key, typed_child] : typed_object)
        {
            auto it = merged_object.find(key);
            if (it == merged_object.end())
            {
                if (fieldContainsEmptyContainerHint(typed_child))
                    merged_object[key] = typed_child;
                continue;
            }

            it->second = restoreTypedEmptyContainers(std::move(it->second), typed_child);
        }

        return merged_field;
    }

    if (typed_field.getType() == Field::Types::Array)
    {
        const auto & typed_array = typed_field.safeGet<Array>();
        if (merged_field.getType() == Field::Types::String && typed_array.empty() && merged_field.safeGet<String>() == "[]")
            return typed_field;

        if (merged_field.getType() != Field::Types::Array)
            return merged_field;

        auto & merged_array = merged_field.safeGet<Array>();
        size_t limit = std::min(merged_array.size(), typed_array.size());
        for (size_t i = 0; i < limit; ++i)
            merged_array[i] = restoreTypedEmptyContainers(std::move(merged_array[i]), typed_array[i]);

        return merged_field;
    }

    return merged_field;
}

void restoreSchemaEmptyContainers(Field & field)
{
    if (field.getType() == Field::Types::String)
    {
        const auto & text = field.safeGet<String>();
        if (text == "{}")
        {
            field = Field(Object {});
            return;
        }
        if (text == "[]")
        {
            field = Field(Array {});
            return;
        }
        return;
    }

    if (field.getType() == Field::Types::Object)
    {
        auto & object = field.safeGet<Object>();
        for (auto & [_, child] : object)
            restoreSchemaEmptyContainers(child);
        return;
    }

    if (field.getType() == Field::Types::Array)
    {
        auto & array = field.safeGet<Array>();
        for (auto & child : array)
            restoreSchemaEmptyContainers(child);
        return;
    }
}

}

MutableColumnPtr formOutputColumn(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    size_t num_rows)
{
    const auto & metadata_column = *row_subgroup.columns.at(output_info.variant_metadata_primitive).column;
    const IColumn * value_column = nullptr;
    if (output_info.variant_value_primitive != UINT64_MAX)
        value_column = row_subgroup.columns.at(output_info.variant_value_primitive).column.get();

    VariantMetadataCache metadata_cache;
    std::optional<VariantMetadata> shared_metadata_storage;
    bool metadata_is_shared_across_rows = true;
    std::optional<std::string_view> first_metadata;
    std::vector<std::optional<std::string_view>> metadata_values(num_rows);
    std::vector<std::optional<std::string_view>> value_values(num_rows);
    for (size_t row = 0; row < num_rows; ++row)
    {
        metadata_values[row] = getNullableStringValue(metadata_column, row);
        if (value_column)
            value_values[row] = getNullableStringValue(*value_column, row);
        auto metadata = metadata_values[row];
        if (row == 0)
        {
            first_metadata = metadata;
            continue;
        }

        if (metadata != first_metadata)
            metadata_is_shared_across_rows = false;
    }

    std::vector<const VariantMetadata *> metadata_by_row(num_rows, nullptr);
    if (metadata_is_shared_across_rows)
    {
        if (first_metadata.has_value())
        {
            shared_metadata_storage.emplace(decodeMetadata(*first_metadata));
            const VariantMetadata * shared_metadata = &*shared_metadata_storage;
            std::fill(metadata_by_row.begin(), metadata_by_row.end(), shared_metadata);
        }
    }
    else
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (metadata_values[row].has_value())
                metadata_by_row[row] = &metadata_cache.get(*metadata_values[row]);
        }
    }

    ColumnPtr typed_value_column;
    const Reader::OutputColumnInfo * typed_value_output_info = nullptr;
    const bool use_exact_typed_value_output
        = !output_info.variant_string_output_uses_json && !isComplexVariantExactOutputType(output_info.output_type);
    if (output_info.variant_typed_value_output != UINT64_MAX)
    {
        typed_value_output_info = &reader.output_columns.at(output_info.variant_typed_value_output);
        typed_value_column = reader.formOutputColumn(
            row_subgroup,
            output_info.variant_typed_value_output,
            num_rows,
            /*skip_cast=*/ !use_exact_typed_value_output);
    }

    DataTypePtr typed_value_input_type = typed_value_output_info ? typed_value_output_info->input_type : nullptr;
    DataTypePtr typed_value_output_type = typed_value_output_info ? typed_value_output_info->output_type : nullptr;
    if (use_exact_typed_value_output && typed_value_output_type)
        typed_value_input_type = typed_value_output_type;
    const bool direct_dynamic_typed_value_compatible
        = !typed_value_output_type || isDirectDynamicCompatibleVariantTypedType(typed_value_output_type);
    const bool normalize_temporal_scalars
        = output_info.variant_string_output_uses_json || isNullableStringType(output_info.output_type.get());

    if (typed_value_column && use_exact_typed_value_output && typed_value_input_type)
    {
        DataTypePtr exact_typed_value_type = output_info.output_type;
        if (typeid_cast<const ColumnNullable *>(typed_value_column.get())
            && !typeid_cast<const DataTypeNullable *>(exact_typed_value_type.get()))
        {
            exact_typed_value_type = makeVariantExactOutputTypeNullable(exact_typed_value_type);
        }

        if (!typed_value_input_type->equals(*exact_typed_value_type))
        {
            auto casted_typed_value_column = castColumn(
                {std::move(typed_value_column), typed_value_input_type, output_info.name + ".__typed_value"},
                exact_typed_value_type);
            typed_value_column = std::move(casted_typed_value_column);
            typed_value_input_type = exact_typed_value_type;
            typed_value_output_type = exact_typed_value_type;
        }
    }

    std::vector<ConvertedTypedField> typed_value_rows;
    if (typed_value_column)
    {
        typed_value_rows = convertTypedColumnRange(
            *typed_value_column,
            typed_value_input_type,
            metadata_by_row.data(),
            num_rows,
            0,
            reader.options.format,
            metadata_is_shared_across_rows,
            (metadata_is_shared_across_rows && num_rows == 1)
                || output_info.variant_preserve_empty_typed_fields
                || (typed_value_output_info && typed_value_output_info->variant_preserve_empty_typed_fields),
            std::nullopt,
            normalize_temporal_scalars);
    }

    auto read_decoded_row = [&](size_t row)
    {
        DecodedRow decoded_row;
        const auto row_value = value_values[row];
        const VariantMetadata * row_metadata = metadata_by_row[row];
        const ConvertedTypedField * prepared_typed_row = typed_value_rows.empty() ? nullptr : &typed_value_rows[row];
        const bool typed_value_present = prepared_typed_row && prepared_typed_row->present;

        if (!row_value.has_value() && !typed_value_present)
        {
            decoded_row.is_logical_null = true;
            return decoded_row;
        }

        if (!row_metadata)
        {
            if (typed_value_present)
                decoded_row.typed_container_is_empty = prepared_typed_row->is_empty_container;

            if (!row_value.has_value() && decoded_row.typed_container_is_empty)
            {
                decoded_row.is_logical_null = true;
                return decoded_row;
            }

            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: row {} has `NULL` `metadata` but non-`NULL` payload", row);
        }

        decoded_row.metadata = row_metadata;

        if (typed_value_present)
        {
            decoded_row.typed_container_is_empty = prepared_typed_row->is_empty_container;
            decoded_row.typed_field = prepared_typed_row->field;
        }

        if (row_value.has_value())
        {
            Field value_field = decodeToField(*row_metadata, *row_value);
            if (decoded_row.typed_field.has_value())
                decoded_row.merged_field = mergeFields(std::move(value_field), *decoded_row.typed_field);
            else
                decoded_row.merged_field = std::move(value_field);
        }
        else if (decoded_row.typed_field.has_value())
        {
            decoded_row.merged_field = decoded_row.typed_field;
        }

        if (decoded_row.merged_field.has_value() && decoded_row.typed_field.has_value())
            decoded_row.merged_field = restoreTypedEmptyContainers(std::move(*decoded_row.merged_field), *decoded_row.typed_field);

        if (decoded_row.merged_field.has_value()
            && (output_info.variant_preserve_empty_typed_fields
                || (typed_value_output_info && typed_value_output_info->variant_preserve_empty_typed_fields)))
        {
            restoreSchemaEmptyContainers(*decoded_row.merged_field);
        }

        return decoded_row;
    };

    MutableColumnPtr result = output_info.output_type->createColumn();
    auto serialization = output_info.output_type->getDefaultSerialization();
    const OutputMode output_mode
        = isNullableStringType(output_info.output_type.get()) && output_info.variant_string_output_uses_json ? OutputMode::NullableString
        : isDynamicLikeVariantOutputType(output_info.output_type.get()) ? OutputMode::Dynamic
        : isObjectLikeVariantOutputType(output_info.output_type.get()) ? OutputMode::Object
        : OutputMode::Generic;
    const bool is_nullable_output = typeid_cast<const DataTypeNullable *>(output_info.output_type.get());
    const bool allow_dynamic_empty_object_hint_insert
        = output_mode == OutputMode::Dynamic
        && (output_info.variant_has_direct_empty_typed_wrapper
            || (typed_value_output_info && typed_value_output_info->variant_has_direct_empty_typed_wrapper));
    constexpr std::string_view null_json = "null";
    String json_buffer;
    ColumnString * string_output = nullptr;
    ColumnNullable * nullable_string_output = nullptr;

    if (output_mode == OutputMode::NullableString)
    {
        if (is_nullable_output)
        {
            nullable_string_output = &assert_cast<ColumnNullable &>(*result);
            string_output = &assert_cast<ColumnString &>(nullable_string_output->getNestedColumn());
            nullable_string_output->getNullMapData().reserve(num_rows);
        }
        else
        {
            string_output = &assert_cast<ColumnString &>(*result);
        }

        string_output->reserve(num_rows);
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        auto decoded_row = read_decoded_row(row);

        if (output_mode == OutputMode::NullableString)
        {
            if (nullable_string_output
                && (!decoded_row.merged_field.has_value() || decoded_row.merged_field->isNull()))
            {
                string_output->insertDefault();
                nullable_string_output->getNullMapData().push_back(UInt8(1));
                continue;
            }

            fillDecodedRowJSON(decoded_row, json_buffer);
            string_output->insertData(json_buffer.data(), json_buffer.size());
            if (nullable_string_output)
                nullable_string_output->getNullMapData().push_back(UInt8(0));
            continue;
        }

        if (decoded_row.is_logical_null)
        {
            if (!output_info.variant_string_output_uses_json)
            {
                result->insertDefault();
                continue;
            }

            if (output_mode != OutputMode::Generic && tryInsertFieldIntoOutput(*result, output_info.output_type, Field(), reader.options.format))
                continue;

            ReadBufferFromString buf(null_json);
            serialization->deserializeTextJSON(*result, buf, reader.options.format);
            continue;
        }

        bool allow_direct_insert
            = decoded_row.merged_field.has_value()
            && (output_mode != OutputMode::Dynamic
                || !decoded_row.typed_field.has_value()
                || direct_dynamic_typed_value_compatible);
        if (allow_direct_insert && tryInsertFieldIntoOutput(*result, output_info.output_type, *decoded_row.merged_field, reader.options.format, allow_dynamic_empty_object_hint_insert))
            continue;

        fillDecodedRowJSON(decoded_row, json_buffer);
        if ((output_mode == OutputMode::Dynamic || is_nullable_output) && json_buffer == "null")
        {
            result->insertDefault();
            continue;
        }

        ReadBufferFromString buf(json_buffer);
        serialization->deserializeTextJSON(*result, buf, reader.options.format);
    }

    row_subgroup.columns.at(output_info.variant_metadata_primitive).column.reset();
    if (output_info.variant_value_primitive != UINT64_MAX)
        row_subgroup.columns.at(output_info.variant_value_primitive).column.reset();
    return result;
}

}
