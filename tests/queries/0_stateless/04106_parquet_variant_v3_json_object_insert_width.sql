SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04106_parquet_variant_v3_json_object_insert_width.parquet', Parquet)
SELECT 1 AS id, CAST('{"a":100}' AS JSON) AS data
UNION ALL
SELECT 2, CAST('{"a":200}' AS JSON)
UNION ALL
SELECT 3, CAST('{"a":300}' AS JSON);

SELECT toJSONString(data)
FROM file(currentDatabase() || '04106_parquet_variant_v3_json_object_insert_width.parquet', Parquet, 'id UInt8, data JSON')
ORDER BY id
FORMAT TSVRaw;

SELECT toJSONString(data)
FROM file(currentDatabase() || '04106_parquet_variant_v3_json_object_insert_width.parquet', Parquet, 'id UInt8, data Nullable(JSON)')
ORDER BY id
FORMAT TSVRaw;
