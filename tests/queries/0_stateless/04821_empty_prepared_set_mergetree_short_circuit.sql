DROP TABLE IF EXISTS empty_prepared_set;

CREATE TABLE empty_prepared_set
(
    id UInt64,
    uuid UUID,
    nullable_uuid Nullable(UUID),
    INDEX uuid_bloom uuid TYPE bloom_filter GRANULARITY 1,
    INDEX nullable_uuid_bloom nullable_uuid TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO empty_prepared_set VALUES
    (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001'),
    (2, '00000000-0000-0000-0000-000000000002', NULL),
    (3, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000003'),
    (4, '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000004');

SELECT count()
FROM empty_prepared_set
WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0))
SETTINGS use_skip_indexes = 1, log_comment = '04821_empty_in', log_queries = 1;

SELECT count()
FROM empty_prepared_set
WHERE id = 1
    AND (uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) OR 0)
SETTINGS use_skip_indexes = 1, log_comment = '04821_nested_empty_in', log_queries = 1;

SELECT count()
FROM empty_prepared_set
WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) OR id = 1
SETTINGS use_skip_indexes = 1, log_comment = '04821_empty_in_or', log_queries = 1;

SELECT count()
FROM empty_prepared_set
WHERE nullable_uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000001') FROM numbers(1));

SELECT count()
FROM empty_prepared_set
WHERE uuid NOT IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0))
SETTINGS use_skip_indexes = 1, log_comment = '04821_empty_not_in', log_queries = 1;

SELECT count()
FROM empty_prepared_set
WHERE id = 1
    AND (uuid NOT IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) OR id = 0)
SETTINGS use_skip_indexes = 1, log_comment = '04821_nested_empty_not_in', log_queries = 1;

SELECT count()
FROM empty_prepared_set
WHERE nullable_uuid NOT IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0));

SELECT count()
FROM empty_prepared_set
WHERE NOT (nullable_uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)));

WITH nullable_uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) AS in_empty
SELECT count()
FROM empty_prepared_set
WHERE in_empty OR NOT in_empty;

SELECT count()
FROM empty_prepared_set
WHERE nullIn(nullable_uuid, (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)));

SELECT count()
FROM empty_prepared_set
WHERE nullIn(nullable_uuid, (SELECT CAST(NULL AS Nullable(UUID)) FROM numbers(1)))
SETTINGS transform_null_in = 1;

SELECT count()
FROM empty_prepared_set
WHERE notNullIn(nullable_uuid, (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)));

DROP TABLE IF EXISTS empty_prepared_set_final;

CREATE TABLE empty_prepared_set_final
(
    id UInt64,
    uuid UUID,
    value UInt64,
    version UInt64,
    INDEX value_minmax value TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (id, uuid)
SETTINGS index_granularity = 1;

INSERT INTO empty_prepared_set_final VALUES
    (1, '00000000-0000-0000-0000-000000000001', 1, 1),
    (1, '00000000-0000-0000-0000-000000000001', 2, 2);

SELECT count()
FROM empty_prepared_set_final FINAL
PREWHERE uuid NOT IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) AND value = 1
SETTINGS apply_prewhere_after_final = 1, use_skip_indexes = 1, use_skip_indexes_if_final = 1;

SELECT count()
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM empty_prepared_set_final FINAL
    PREWHERE uuid NOT IN (SELECT toUUID('00000000-0000-0000-0000-000000000000') FROM numbers(0)) AND value = 1
    SETTINGS apply_prewhere_after_final = 1, use_skip_indexes = 1, use_skip_indexes_if_final = 1
)
WHERE explain LIKE '%Name: value_minmax%';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['IndexAnalysisRounds'],
    ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - INTERVAL 10 MINUTE
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('04821_empty_in', '04821_empty_in_or', '04821_empty_not_in', '04821_nested_empty_in', '04821_nested_empty_not_in')
ORDER BY log_comment;

DROP TABLE empty_prepared_set;
DROP TABLE empty_prepared_set_final;
