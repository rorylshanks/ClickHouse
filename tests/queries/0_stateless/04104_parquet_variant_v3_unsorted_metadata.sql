SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_verify_checksums = 0;

SELECT id, toJSONString(v)
FROM file('tests/queries/0_stateless/data_parquet/04104_parquet_variant_v3_unsorted_metadata.parquet', Parquet, 'id UInt64, v JSON')
ORDER BY id
FORMAT TSVRaw;
