create table prc_bn_1m_candle_set
(   part_name   Date,
    target_price_in_in_usd Float64,
    gain_1_pct Float64,
    chery_max_avg_price_in_usd Float64,
    target_name LowCardinality(String),
    target_value Float32,
    target_timestamp DateTime,
    target_symbol LowCardinality(String),
    feature_sum_of_posetiv_changes_in_backlist_for_avg_prices_changed_per_1minute_pct Float64,
    feature_sum_of_negativ_changes_in_backlist_for_avg_prices_changed_per_1minute_pct Float64,
    feature_sum_of_changes_in_backlist_for_avg_prices_changed_per_1minute_pct Float64,
    avg_vol_per_min Float64,
    row_version DateTime materialized now()
)
    engine = ReplacingMergeTree(row_version)
        PARTITION BY part_name
        ORDER BY (part_name, target_name, target_symbol, target_timestamp)
        SETTINGS index_granularity = 8192;