SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_verify_checksums = 0;

SELECT *
FROM file('tests/queries/0_stateless/data_parquet/04101_parquet_variant_v3_bad_metadata_offsets.parquet', Parquet, 'id UInt64, v Dynamic'); -- { serverError INCORRECT_DATA }

SELECT *
FROM file('tests/queries/0_stateless/data_parquet/04101_parquet_variant_v3_bad_field_id.parquet', Parquet, 'id UInt64, v Dynamic'); -- { serverError INCORRECT_DATA }
