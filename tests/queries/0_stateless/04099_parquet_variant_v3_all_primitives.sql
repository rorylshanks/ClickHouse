SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

CREATE TABLE primitive_dyn_src_04099
(
    id UInt64,
    v Dynamic
)
ENGINE = Memory;

INSERT INTO primitive_dyn_src_04099
SELECT 1, CAST(true AS Dynamic)
UNION ALL
SELECT 2, CAST(toInt8(-5) AS Dynamic)
UNION ALL
SELECT 3, CAST(toInt16(-300) AS Dynamic)
UNION ALL
SELECT 4, CAST(toInt32(-70000) AS Dynamic)
UNION ALL
SELECT 5, CAST(toInt64(-5000000000) AS Dynamic)
UNION ALL
SELECT 6, CAST(toFloat32(1.5) AS Dynamic)
UNION ALL
SELECT 7, CAST(toFloat64(-2.75) AS Dynamic)
UNION ALL
SELECT 8, CAST('hello' AS Dynamic)
UNION ALL
SELECT 9, CAST(CAST('12.34' AS Decimal32(2)) AS Dynamic)
UNION ALL
SELECT 10, CAST(CAST('-56.789' AS Decimal64(3)) AS Dynamic)
UNION ALL
SELECT 11, CAST(CAST('1234567890123.4567' AS Decimal128(4)) AS Dynamic)
UNION ALL
SELECT 12, CAST(toUUID('550e8400-e29b-41d4-a716-446655440000') AS Dynamic)
UNION ALL
SELECT 13, CAST(toIPv4('203.0.113.42') AS Dynamic)
UNION ALL
SELECT 14, CAST(toIPv6('2001:db8::1') AS Dynamic)
UNION ALL
SELECT 15, CAST(toDate('2024-01-02') AS Dynamic)
UNION ALL
SELECT 16, CAST(toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC') AS Dynamic)
UNION ALL
SELECT 17, CAST(CAST('04:05:06.1234567' AS Time64(7)) AS Dynamic);

INSERT INTO FUNCTION file(currentDatabase() || '04099_parquet_variant_v3_all_primitives.parquet', Parquet)
SELECT *
FROM primitive_dyn_src_04099
ORDER BY id;

SELECT tupleElement(col, 1), tupleElement(col, 2), tupleElement(col, 5), tupleElement(col, 6)
FROM file(currentDatabase() || '04099_parquet_variant_v3_all_primitives.parquet', ParquetMetadata)
ARRAY JOIN columns AS col
ORDER BY tupleElement(col, 2)
FORMAT TSVRaw;

SELECT id, dynamicType(v), toJSONString(v)
FROM file(currentDatabase() || '04099_parquet_variant_v3_all_primitives.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;
