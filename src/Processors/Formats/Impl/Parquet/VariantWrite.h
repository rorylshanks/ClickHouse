#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

namespace DB::Parquet
{

struct PreparedVariantColumns
{
    ColumnPtr metadata_column;
    DataTypePtr metadata_type;
    ColumnPtr value_column;
    DataTypePtr value_type;
    ColumnPtr typed_value_column;
    DataTypePtr typed_value_type;
};

/// Prepare Variant-encoded columns for Parquet output.
///
/// If `shredded_type` is null and `out_shredded_type` is non-null, the function
/// infers a shredded type from the data and writes it to `*out_shredded_type`.
/// The inferred type is also used for encoding in the same call, so inference
/// and encoding happen in a single pass over the data.
///
/// The metadata dictionary is built once from the union of all object keys across
/// all rows in the column chunk, then shared by every row.
PreparedVariantColumns prepareVariantColumnsForWrite(
    const ColumnPtr & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    DataTypePtr shredded_type,
    DataTypePtr * out_shredded_type = nullptr);

}
