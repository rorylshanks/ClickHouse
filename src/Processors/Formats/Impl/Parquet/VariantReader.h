#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>

#include <optional>

namespace DB::Parquet::VariantReader
{

const FormatSettings & getDefaultJSONFormatSettings();

struct VariantMetadata
{
    std::vector<std::string_view> strings;
    bool strings_sorted = false;
};

VariantMetadata decodeMetadata(std::string_view metadata_blob);

struct VariantMetadataCache
{
    const VariantMetadata & get(std::string_view metadata_blob);

    std::unordered_map<String, VariantMetadata> cache;
};

struct DecodedRow
{
    bool is_logical_null = false;
    bool typed_container_is_empty = false;
    const VariantMetadata * metadata = nullptr;
    std::optional<Field> typed_field;
    std::optional<Field> merged_field;
};

struct ConvertedTypedField
{
    bool present = false;
    bool is_empty_container = false;
    Field field;
};

Field decodeToField(const VariantMetadata & metadata, std::string_view value_blob);
Field mergeFields(Field base, const Field & patch);
void fillDecodedRowJSON(const DecodedRow & decoded_row, String & out);

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
    bool normalize_temporal_scalars);

bool tryInsertFieldIntoOutput(
    IColumn & output_column,
    const DataTypePtr & output_type,
    const Field & field,
    const FormatSettings & format_settings,
    bool allow_dynamic_empty_object_hint_insert = false);

MutableColumnPtr formOutputColumn(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    size_t num_rows);

}
