SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04107_parquet_variant_v3_wrapper_residual_pushdown.parquet', Parquet)
SELECT 1 AS id, CAST('{"a":1000}' AS JSON) AS data
UNION ALL
SELECT 2, CAST('{"a":2000}' AS JSON)
UNION ALL
SELECT 3, CAST('{"a":3000}' AS JSON)
UNION ALL
SELECT 4, CAST('{"a":"LOST_STRING"}' AS JSON)
UNION ALL
SELECT 5, CAST('{"a":[]}' AS JSON);

SELECT toJSONString(data)
FROM file(currentDatabase() || '04107_parquet_variant_v3_wrapper_residual_pushdown.parquet', Parquet, 'id UInt8, data JSON')
ORDER BY id
FORMAT TSVRaw;

SELECT data.a
FROM file(currentDatabase() || '04107_parquet_variant_v3_wrapper_residual_pushdown.parquet', Parquet, 'id UInt8, data JSON(a String)')
ORDER BY id
FORMAT TSVRaw;
