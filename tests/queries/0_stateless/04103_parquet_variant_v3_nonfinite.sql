SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_verify_checksums = 0;

SELECT
    id,
    toTypeName(v),
    dynamicType(v),
    dynamicElement(v, 'Float64'),
    isNaN(dynamicElement(v, 'Float64')),
    isInfinite(dynamicElement(v, 'Float64'))
FROM file('tests/queries/0_stateless/data_parquet/04103_parquet_variant_v3_nonfinite.parquet', Parquet, 'id UInt64, v Dynamic')
ORDER BY id
FORMAT TSVRaw;

SELECT id, v
FROM file('tests/queries/0_stateless/data_parquet/04103_parquet_variant_v3_nonfinite.parquet', Parquet, 'id UInt64, v String')
ORDER BY id
FORMAT TSVRaw;
