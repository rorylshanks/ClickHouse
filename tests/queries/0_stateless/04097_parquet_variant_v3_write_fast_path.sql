SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04097_parquet_variant_v3_write_fast_path_json.parquet', Parquet)
SELECT 1::UInt64 AS id, CAST('{"a.b":"x","nested":{"c.d":[1,{"e.f":"y"}]}}' AS JSON(`a.b` String, known UInt64)) AS var
UNION ALL
SELECT 2::UInt64, CAST('{"known":5,"nested":{"c.d":[]}}' AS JSON(`a.b` String, known UInt64));

SELECT id, toJSONString(var)
FROM file(currentDatabase() || '04097_parquet_variant_v3_write_fast_path_json.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;

INSERT INTO FUNCTION file(currentDatabase() || '04097_parquet_variant_v3_write_fast_path_fallback.parquet', Parquet)
SELECT 1::UInt64 AS id, CAST('{"d":"2024-01-02"}' AS JSON(d Date)) AS var;

SELECT id, toJSONString(var)
FROM file(currentDatabase() || '04097_parquet_variant_v3_write_fast_path_fallback.parquet', Parquet)
ORDER BY id
FORMAT TSVRaw;
