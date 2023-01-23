create table prc_bn_1m_candle_targets
(
    target_name LowCardinality(String),
    symbol      LowCardinality(String),
    part_name   Date,
    time_open   DateTime,
    value       Float32,
    row_version DateTime materialized now()
)
    engine = ReplacingMergeTree(row_version)
        PARTITION BY part_name
        ORDER BY (target_name, symbol, part_name, time_open)
        SETTINGS index_granularity = 8192;