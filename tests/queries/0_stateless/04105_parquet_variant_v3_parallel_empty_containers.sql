SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_parallel_encoding = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET max_threads = 4;

INSERT INTO FUNCTION file(currentDatabase() || '04105_parquet_variant_v3_parallel_empty_containers.parquet', Parquet)
SELECT 1::UInt64 AS id, CAST('{}' AS JSON) AS v
UNION ALL
SELECT 2::UInt64, CAST('{"a":[]}' AS JSON)
UNION ALL
SELECT 3::UInt64, CAST('{"a":{"b":1}}' AS JSON)
UNION ALL
SELECT 4::UInt64, CAST('{"a":[{"b":2}]}' AS JSON);

SELECT id, toJSONString(v)
FROM file(currentDatabase() || '04105_parquet_variant_v3_parallel_empty_containers.parquet', Parquet, 'id UInt64, v JSON')
ORDER BY id
FORMAT TSVRaw;
