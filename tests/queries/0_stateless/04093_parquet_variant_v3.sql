SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET schema_inference_make_columns_nullable = 'auto';

INSERT INTO FUNCTION file(currentDatabase() || '04093_parquet_variant_v3_json.parquet', Parquet)
SELECT 1::UInt64 AS id, CAST('{"a":1,"c":["x",2],"extra":"keep"}' AS JSON) AS var
UNION ALL
SELECT 2::UInt64, CAST('{"a":2,"c":[{"z":3}]}' AS JSON)
UNION ALL
SELECT 3::UInt64, CAST('{"a":3,"extra":"x"}' AS JSON)
UNION ALL
SELECT 4::UInt64, CAST('{"a":4,"another":{"q":1},"c":[1,2,3]}' AS JSON);

SELECT c.name, c.path, c.logical_type
FROM file(currentDatabase() || '04093_parquet_variant_v3_json.parquet', ParquetMetadata)
ARRAY JOIN columns AS c
ORDER BY c.path
FORMAT TSVRaw;

SELECT id, toJSONString(var)
FROM file(currentDatabase() || '04093_parquet_variant_v3_json.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;

SELECT id, dynamicType(var), toJSONString(var)
FROM file(currentDatabase() || '04093_parquet_variant_v3_json.parquet', Parquet, 'id UInt64, var Dynamic')
ORDER BY id
FORMAT TSVRaw;

INSERT INTO FUNCTION file(currentDatabase() || '04093_parquet_variant_v3_dynamic.parquet', Parquet)
SELECT 1::UInt64 AS id, CAST((1, [CAST('x' AS Dynamic), CAST(2 AS Dynamic)], 'keep') AS Tuple(a UInt64, c Array(Dynamic), extra String))::Dynamic AS var
UNION ALL
SELECT 2::UInt64, CAST((2, CAST([CAST(CAST(tuple(3) AS Tuple(z UInt64)) AS Dynamic)] AS Array(Dynamic)), CAST(NULL AS Nullable(String))) AS Tuple(a UInt64, c Array(Dynamic), extra Nullable(String)))::Dynamic
UNION ALL
SELECT 3::UInt64, CAST('iceberg' AS Dynamic)
UNION ALL
SELECT 4::UInt64, CAST([CAST(1 AS Dynamic), CAST(CAST((4, 'x') AS Tuple(a UInt64, extra String)) AS Dynamic), CAST(NULL AS Dynamic)] AS Array(Dynamic))::Dynamic
UNION ALL
SELECT 5::UInt64, CAST(NULL AS Dynamic);

SELECT id, dynamicType(var), toJSONString(var)
FROM file(currentDatabase() || '04093_parquet_variant_v3_dynamic.parquet', Parquet, 'id UInt64, var Dynamic')
ORDER BY id
FORMAT TSVRaw;
