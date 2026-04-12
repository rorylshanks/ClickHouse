SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04096_parquet_variant_v3_pushdown.parquet', Parquet)
SELECT CAST('{"kind":"commit","did":"did:plc:a","time_us":1,"commit":{"collection":"app.bsky.feed.post","operation":"create"}}' AS JSON) AS data
UNION ALL
SELECT CAST('{"kind":"commit","did":"did:plc:b","time_us":2,"commit":{"collection":"app.bsky.feed.like","operation":"delete"}}' AS JSON)
UNION ALL
SELECT CAST('{"kind":"identity","did":"did:plc:c","time_us":3}' AS JSON);

SELECT
    data.kind,
    data.did,
    data.commit.collection,
    data.commit.operation,
    data.time_us
FROM file(
    currentDatabase() || '04096_parquet_variant_v3_pushdown.parquet',
    Parquet,
    'data JSON(kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
ORDER BY data.did
FORMAT TSVRaw;
