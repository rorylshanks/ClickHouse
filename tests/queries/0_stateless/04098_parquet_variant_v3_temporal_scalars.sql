SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_json.parquet', Parquet)
SELECT
    1::UInt64 AS id,
    CAST(
        '{"d":"2024-01-02","d32":"1901-01-02","dt":"2024-01-02 03:04:05","dt64":"2024-01-02 03:04:05.123456","tm":"01:02:03","tm64":"04:05:06.1234567","dec":"123.456","uuid":"550e8400-e29b-41d4-a716-446655440000"}'
        AS JSON(
            d Date,
            d32 Date32,
            dt DateTime('UTC'),
            dt64 DateTime64(6, 'UTC'),
            tm Time,
            tm64 Time64(7),
            dec Decimal64(3),
            uuid UUID))
        AS var;

SELECT tupleElement(col, 1), tupleElement(col, 2), tupleElement(col, 5), tupleElement(col, 6)
FROM file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_json.parquet', ParquetMetadata)
ARRAY JOIN columns AS col
WHERE tupleElement(col, 2) LIKE 'var.typed_value.%'
ORDER BY tupleElement(col, 2)
FORMAT TSVRaw;

SELECT id, toJSONString(var)
FROM file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_json.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;

CREATE TABLE temporal_dyn_src_04098
(
    id UInt64,
    v Dynamic
) ENGINE = Memory;

INSERT INTO temporal_dyn_src_04098
SELECT 1, CAST(toDate('2024-01-02') AS Dynamic)
UNION ALL
SELECT 2, CAST(toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC') AS Dynamic)
UNION ALL
SELECT 3, CAST(CAST('04:05:06.1234567' AS Time64(7)) AS Dynamic);

INSERT INTO FUNCTION file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_dynamic.parquet', Parquet)
SELECT *
FROM temporal_dyn_src_04098
ORDER BY id;

SELECT tupleElement(col, 1), tupleElement(col, 2), tupleElement(col, 5), tupleElement(col, 6)
FROM file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_dynamic.parquet', ParquetMetadata)
ARRAY JOIN columns AS col
ORDER BY tupleElement(col, 2)
FORMAT TSVRaw;

SELECT id, dynamicType(v), toJSONString(v)
FROM file(currentDatabase() || '04098_parquet_variant_v3_temporal_scalars_dynamic.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;
