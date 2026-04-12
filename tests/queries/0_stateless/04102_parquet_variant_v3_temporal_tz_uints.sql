SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04102_parquet_variant_v3_temporal_tz_uints.parquet', Parquet)
SELECT
    1::UInt64 AS id,
    CAST(
        '{"dt_local":"2024-01-02 03:04:05","dt_utc":"2024-01-02 03:04:05","dt_ny":"2024-01-02 03:04:05","u8":255,"u16":65535,"u32":4294967295}'
        AS JSON(
            dt_local DateTime,
            dt_utc DateTime('UTC'),
            dt_ny DateTime('America/New_York'),
            u8 UInt8,
            u16 UInt16,
            u32 UInt32))
        AS var;

SELECT tupleElement(col, 1), tupleElement(col, 2), tupleElement(col, 5), tupleElement(col, 6)
FROM file(currentDatabase() || '04102_parquet_variant_v3_temporal_tz_uints.parquet', ParquetMetadata)
ARRAY JOIN columns AS col
WHERE tupleElement(col, 2) LIKE 'var.typed_value.%'
ORDER BY tupleElement(col, 2)
FORMAT TSVRaw;

SELECT
    toTypeName(var.dt_local), toString(var.dt_local),
    toTypeName(var.dt_utc), toString(var.dt_utc),
    toTypeName(var.dt_ny), toString(var.dt_ny),
    toTypeName(var.u8), toUInt64(var.u8),
    toTypeName(var.u16), toUInt64(var.u16),
    toTypeName(var.u32), toUInt64(var.u32)
FROM file(
    currentDatabase() || '04102_parquet_variant_v3_temporal_tz_uints.parquet',
    Parquet,
    '`id` UInt64, `var` JSON(dt_local DateTime, dt_utc DateTime(\'UTC\'), dt_ny DateTime(\'America/New_York\'), u8 UInt8, u16 UInt16, u32 UInt32)')
FORMAT TSVRaw;

CREATE TABLE temporal_scale_src_04102
(
    id UInt64,
    v Dynamic
) ENGINE = Memory;

INSERT INTO temporal_scale_src_04102
SELECT 1, CAST(toDateTime64('2024-01-02 03:04:05.1234567', 7, 'UTC') AS Dynamic)
UNION ALL
SELECT 2, CAST(toDateTime64('2024-01-02 03:04:05.12345678', 8) AS Dynamic)
UNION ALL
SELECT 3, CAST(toDateTime64('2024-01-02 03:04:05.123456789', 9, 'UTC') AS Dynamic);

INSERT INTO FUNCTION file(currentDatabase() || '04102_parquet_variant_v3_temporal_scales.parquet', Parquet)
SELECT *
FROM temporal_scale_src_04102
ORDER BY id;

SELECT id, dynamicType(v), toJSONString(v)
FROM file(currentDatabase() || '04102_parquet_variant_v3_temporal_scales.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;
