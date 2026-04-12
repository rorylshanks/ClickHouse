SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET schema_inference_make_columns_nullable = 'auto';

INSERT INTO FUNCTION file(currentDatabase() || '04094_parquet_variant_v3_correctness.parquet', Parquet)
SELECT
    1::UInt64 AS id,
    CAST(CAST((1::UInt64, 2::UInt64) AS Tuple(value UInt64, typed_value UInt64)) AS Dynamic) AS var
UNION ALL
SELECT
    2::UInt64,
    CAST(
        CAST(
            (
                CAST((3::UInt64, 4::UInt64) AS Tuple(value UInt64, typed_value UInt64)),
                'ok'
            ) AS Tuple(nested Tuple(value UInt64, typed_value UInt64), tag String)
        ) AS Dynamic)
UNION ALL
SELECT
    3::UInt64,
    CAST([CAST(1 AS Dynamic), CAST('x' AS Dynamic)] AS Array(Dynamic))::Dynamic
UNION ALL
SELECT
    4::UInt64,
    CAST('plain string' AS Dynamic)
UNION ALL
SELECT
    5::UInt64,
    CAST(NULL AS Dynamic);

SELECT id, toTypeName(var), dynamicType(var), toJSONString(var)
FROM file(currentDatabase() || '04094_parquet_variant_v3_correctness.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;

SELECT id, var
FROM file(currentDatabase() || '04094_parquet_variant_v3_correctness.parquet', Parquet, 'id UInt64, var String')
ORDER BY id
FORMAT TSVRaw;

INSERT INTO FUNCTION file(currentDatabase() || '04094_parquet_variant_v3_nullable_json.parquet', Parquet)
SELECT
    1::UInt64 AS id,
    CAST(NULL AS Nullable(JSON)) AS var
UNION ALL
SELECT
    2::UInt64,
    CAST(CAST('{"a":10}' AS JSON) AS Nullable(JSON))
UNION ALL
SELECT
    3::UInt64,
    CAST(CAST('{"typed_value":11,"value":10}' AS JSON) AS Nullable(JSON));

SELECT
    id,
    isNull(var),
    if(isNull(var), 'NULL', toJSONString(assumeNotNull(var)))
FROM file(currentDatabase() || '04094_parquet_variant_v3_nullable_json.parquet', Parquet, 'id UInt64, var Nullable(JSON)')
ORDER BY id
FORMAT TSVRaw;
