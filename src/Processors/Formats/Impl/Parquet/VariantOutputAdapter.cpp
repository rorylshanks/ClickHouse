#include <Processors/Formats/Impl/Parquet/VariantReader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Interpreters/convertFieldToType.h>

#include <cmath>

namespace DB::Parquet::VariantReader
{

namespace
{

bool fieldContainsEmptyObjectHint(const Field & field)
{
    if (field.getType() == Field::Types::Object)
    {
        const auto & object = field.safeGet<Object>();
        if (object.empty())
            return true;

        for (const auto & [_, child] : object)
        {
            if (fieldContainsEmptyObjectHint(child))
                return true;
        }

        return false;
    }

    if (field.getType() == Field::Types::Array)
    {
        const auto & array = field.safeGet<Array>();
        for (const auto & child : array)
        {
            if (fieldContainsEmptyObjectHint(child))
                return true;
        }

        return false;
    }

    return false;
}

bool tryInsertConvertedFieldIntoColumn(
    IColumn & output_column,
    const DataTypePtr & output_type,
    const Field & field,
    const FormatSettings & format_settings)
{
    Field converted = convertFieldToType(field, *output_type, nullptr, format_settings);
    if (converted.isNull() && !field.isNull())
        return false;

    return output_column.tryInsert(converted);
}

bool tryInsertFieldIntoPlainStringOutput(IColumn & output_column, const Field & field)
{
    auto insert_string = [](ColumnString & string_column, std::string_view value)
    {
        string_column.insertData(value.data(), value.size());
    };

    String text;
    if (field.getType() == Field::Types::String)
    {
        text = field.safeGet<String>();
    }
    else if (field.getType() == Field::Types::Bool)
    {
        text = field.safeGet<bool>() ? "true" : "false";
    }
    else if (field.getType() == Field::Types::Null)
    {
        text = "null";
    }
    else
    {
        DecodedRow decoded_row;
        decoded_row.merged_field = field;
        fillDecodedRowJSON(decoded_row, text);
    }

    if (auto * nullable = typeid_cast<ColumnNullable *>(&output_column))
    {
        auto & nested = assert_cast<ColumnString &>(nullable->getNestedColumn());
        insert_string(nested, text);
        nullable->getNullMapData().push_back(UInt8(0));
        return true;
    }

    insert_string(assert_cast<ColumnString &>(output_column), text);
    return true;
}

bool canDirectInsertIntoDynamicOutput(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Object:
            return field.safeGet<Object>().empty();
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
        case Field::Types::UUID:
            return true;
        case Field::Types::Float64:
            return !std::isfinite(field.safeGet<Float64>());
        default:
            return false;
    }
}

bool tryInsertFieldIntoDynamicOutput(IColumn & output_column, const Field & field, bool allow_empty_object_hint_insert)
{
    if (!canDirectInsertIntoDynamicOutput(field)
        && !(allow_empty_object_hint_insert && fieldContainsEmptyObjectHint(field)))
    {
        return false;
    }

    return output_column.tryInsert(field);
}

}

bool tryInsertFieldIntoOutput(
    IColumn & output_column,
    const DataTypePtr & output_type,
    const Field & field,
    const FormatSettings & format_settings,
    bool allow_dynamic_empty_object_hint_insert)
{
    if (field.isNull())
    {
        if (typeid_cast<ColumnNullable *>(&output_column) || isDynamicLikeVariantOutputType(output_type.get()))
        {
            output_column.insertDefault();
            return true;
        }

        return false;
    }

    /// Keep `Object` and object-containing `Dynamic` values on the shared JSON path in `VariantReader`.
    if (isDynamicLikeVariantOutputType(output_type.get()))
        return tryInsertFieldIntoDynamicOutput(output_column, field, allow_dynamic_empty_object_hint_insert);

    if (isObjectLikeVariantOutputType(output_type.get()))
        return false;

    if (isNullableStringType(output_type.get()))
        return tryInsertFieldIntoPlainStringOutput(output_column, field);

    return tryInsertConvertedFieldIntoColumn(output_column, output_type, field, format_settings);
}

}
