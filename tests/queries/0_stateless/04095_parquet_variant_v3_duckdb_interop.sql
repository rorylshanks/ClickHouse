SET input_format_parquet_use_native_reader_v3 = 1;

SELECT toTypeName(var), dynamicType(var), toJSONString(var)
FROM file('tests/queries/0_stateless/data_parquet/04095_duckdb_variant_shredded_object2.parquet', Parquet)
FORMAT TSVRaw;

SELECT toTypeName(var), dynamicType(var), toJSONString(var)
FROM file('tests/queries/0_stateless/data_parquet/04095_duckdb_variant_empty_array.parquet', Parquet)
FORMAT TSVRaw;

SELECT toTypeName(var), dynamicType(var), toJSONString(var)
FROM file('tests/queries/0_stateless/data_parquet/04095_duckdb_variant_object_missing_vs_empty_array.parquet', Parquet)
ORDER BY toJSONString(var)
FORMAT TSVRaw;
