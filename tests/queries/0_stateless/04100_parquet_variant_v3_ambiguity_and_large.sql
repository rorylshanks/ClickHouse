SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

CREATE TABLE ambiguous_dyn_src_04100
(
    id UInt64,
    v Dynamic
)
ENGINE = Memory;

INSERT INTO ambiguous_dyn_src_04100
SELECT 1, CAST(1 AS Dynamic)
UNION ALL
SELECT 2, CAST('x' AS Dynamic)
UNION ALL
SELECT 3, CAST(2 AS Dynamic)
UNION ALL
SELECT 4, CAST('y' AS Dynamic);

INSERT INTO FUNCTION file(currentDatabase() || '04100_parquet_variant_v3_ambiguous.parquet', Parquet)
SELECT *
FROM ambiguous_dyn_src_04100
ORDER BY id;

SELECT tupleElement(col, 1), tupleElement(col, 2), tupleElement(col, 5), tupleElement(col, 6)
FROM file(currentDatabase() || '04100_parquet_variant_v3_ambiguous.parquet', ParquetMetadata)
ARRAY JOIN columns AS col
ORDER BY tupleElement(col, 2)
FORMAT TSVRaw;

SELECT id, dynamicType(v), toJSONString(v)
FROM file(currentDatabase() || '04100_parquet_variant_v3_ambiguous.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;

CREATE TABLE large_dyn_src_04100
(
    id UInt64,
    v Dynamic
)
ENGINE = Memory;

INSERT INTO large_dyn_src_04100
SELECT
    1,
    CAST(
        mapFromArrays(
            arrayMap(i -> concat('k', toString(i)), range(300)),
            arrayMap(i -> toUInt64(i), range(300)))
        AS Dynamic)
UNION ALL
SELECT
    2,
    CAST(arrayMap(i -> toUInt64(i), range(300)) AS Dynamic);

INSERT INTO FUNCTION file(currentDatabase() || '04100_parquet_variant_v3_large.parquet', Parquet)
SELECT *
FROM large_dyn_src_04100
ORDER BY id;

SELECT
    id,
    if(id = 1, length(JSONExtractKeys(v)), length(JSONExtractArrayRaw(v))) AS element_count
FROM file(currentDatabase() || '04100_parquet_variant_v3_large.parquet', Parquet, 'id UInt64, v String')
ORDER BY id
FORMAT TSVRaw;
