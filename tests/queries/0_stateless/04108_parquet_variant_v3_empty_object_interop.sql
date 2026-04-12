SET input_format_parquet_use_native_reader_v3 = 1;

SELECT toTypeName(v), dynamicType(v), toJSONString(v)
FROM file('tests/queries/0_stateless/data_parquet/04108_duckdb_variant_empty_object.parquet', Parquet)
FORMAT TSVRaw;

SELECT toTypeName(v), dynamicType(v), toJSONString(v)
FROM file('tests/queries/0_stateless/data_parquet/04108_duckdb_variant_nested_empty_object.parquet', Parquet)
FORMAT TSVRaw;

SELECT toJSONString(v), toTypeName(v)
FROM file('tests/queries/0_stateless/data_parquet/04108_duckdb_variant_nested_empty_object.parquet', Parquet, 'id Int32, v JSON(a JSON)')
FORMAT TSVRaw;

SELECT toTypeName(v), dynamicType(v), toJSONString(v)
FROM file('tests/queries/0_stateless/data_parquet/04108_duckdb_variant_typed_only_wrapper.parquet', Parquet)
FORMAT TSVRaw;

SELECT toJSONString(data), data.a
FROM file('tests/queries/0_stateless/data_parquet/04108_duckdb_variant_typed_only_wrapper.parquet', Parquet, 'id Int32, data JSON(a Array(Int32))')
FORMAT TSVRaw;
