#pragma once
#include <Core/Block.h>
#include <Storages/StorageSnapshot.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/Serialization.h>

namespace DB
{
    struct PrewhereInfo;
    using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

    struct FilterDAGInfo;
    using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

    struct ReadFromFormatInfo
    {
        /// Header that will return Source from storage.
        /// Concatenation of requested_columns and requested_virtual_columns.
        Block source_header;
        /// Header that will be passed to IInputFormat to read data from file.
        /// Superset of requested_columns.
        /// It can contain more columns than were requested if format doesn't support
        /// reading subset of columns.
        Block format_header;
        /// Description of columns for format_header. Used for inserting defaults.
        ColumnsDescription columns_description;
        /// Columns to request from IInputFormat.
        /// Includes columns to read from file and columns outputted by the prewhere expression
        /// (the prewhere columns are not necessarily at the end of the list).
        /// Doesn't include columns that are only used as prewhere input; IInputFormat should deduce
        /// them from prewhere expression.
        NamesAndTypesList requested_columns;
        /// The list of requested virtual columns.
        NamesAndTypesList requested_virtual_columns;
        /// Hints for the serialization of columns.
        /// For example can be retrieved from the destination table in INSERT SELECT query.
        SerializationInfoByName serialization_hints{{}};

        void serialize(IQueryPlanStep::Serialization & ctx) const;
        static ReadFromFormatInfo deserialize(IQueryPlanStep::Deserialization & ctx);

        /// The list of hive partition columns. It shall be read from the path regardless if it is present in the file
        NamesAndTypesList hive_partition_columns_to_read_from_file_path;
        PrewhereInfoPtr prewhere_info;
        FilterDAGInfoPtr row_level_filter;
    };

    struct PrepareReadingFromFormatHiveParams
    {
        /// Columns which exist inside data file.
        NamesAndTypesList file_columns;
        /// Columns which are read from path to data file.
        /// (Hive partition columns).
        std::unordered_map<std::string, DataTypePtr> hive_partition_columns_to_read_from_file_path_map;
    };

    /// Get all needed information for reading from data in some input format.
    ///
    /// `supports_tuple_elements` controls how subcolumn access is handled, e.g. `t.x`.
    /// If true, we'll request supported subcolumns directly from the format, expecting it to
    /// interpret the dot correctly. Today this includes tuple elements and fixed subcolumns of
    /// `JSON` / `Dynamic` columns in `ParquetV3BlockInputFormat`.
    /// If false, or if the subcolumn kind is not supported by the format, we'll ask the format to
    /// read the whole parent column; then the needed subcolumns are extracted by
    /// `ExtractColumnsTransform` later in the pipeline.
    /// Special subcolumns like `nullable.null` still require reading the whole parent column.
    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements = false,
        const PrepareReadingFromFormatHiveParams & hive_parameters = {});

    /// Returns columns_to_read from file.
    Names filterTupleColumnsToRead(NamesAndTypesList & requested_columns);

    ReadFromFormatInfo updateFormatPrewhereInfo(const ReadFromFormatInfo & info, const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info);

    /// Returns the serialization hints from the insertion table (if it's set in the Context).
    SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);
}
