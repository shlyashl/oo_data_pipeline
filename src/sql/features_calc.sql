select
    target_name,
    target_value,
    target_timestamp,
    symbol,
    round
        (
            sumIf
                (
                    feature_avg_prices_changed_per_1minute_pct,
                    feature_avg_prices_changed_per_1minute_pct > 0 and
                    interval_id in (0, -1)
                ), 2
        ) as feature_sum_of_posetiv_changes_in_backlist_for_avg_prices_changed_per_1minute_pct,
    round
        (
            sumIf
                (
                    feature_avg_prices_changed_per_1minute_pct,
                    feature_avg_prices_changed_per_1minute_pct < 0 and
                    interval_id in (0, -1)
                ), 2
        ) as feature_sum_of_negativ_changes_in_backlist_for_avg_prices_changed_per_1minute_pct,
    round
        (
            sumIf
                (
                    feature_avg_prices_changed_per_1minute_pct,
                    interval_id in (0, -1)
                ), 2
        ) as feature_sum_of_changes_in_backlist_for_avg_prices_changed_per_1minute_pct

from
    (
        with
            if(volume_in_usd*volume_in_coins != 0, volume_in_usd/volume_in_coins, 0) as avg_price,
            if(target_id - neighbor(target_id, 1) != 0, 0, 1) as is_last_row_in_target,
            floor
                (
                    1000 * if
                        (
                            is_last_row_in_target * neighbor(avg_price, 1) != 0,
                            ((is_last_row_in_target * avg_price * 100) / neighbor(avg_price, 1)) - 100,
                            0
                        )
                ) / 1000 as feature_avg_prices_changed_per_1minute_pct
        select
            target_name,
            target_value,
            target_timestamp,
            interval_id,
            symbol,
            timestamp,
            feature_avg_prices_changed_per_1minute_pct
        from
            (
                select *
                from
                    (
                        select
                            target_name,
                            value as target_value,
                            murmurHash2_32(time_open, symbol) as target_id,
                            time_open as target_timestamp,
                            symbol,
                            arrayJoin
                                (
                                    arrayMap
                                        (
                                            x -> time_open - interval x-3 minute,
                                            range(10)
                                        )
                                ) as timestamp,
                            multiIf
                                (
                                    time_open > timestamp, -1,
                                    time_open < timestamp - interval 1 minute, 2,
                                    time_open = timestamp - interval 1 minute, 1,
                                    0
                                ) as interval_id
                        from prc_bn_1m_candle_targets
                        where 1=1
                            and toDate(time_open) = today() - 1
                            and target_name = 'price_increase_1'
                            and (symbol, time_open) in
                                (
                                    ('ACHUSDT', '2023-02-02 02:18:00'),
                                    ('ASTRUSDT', '2023-02-02 02:38:00')
                                )
                    ) as targets
                    any left join
                        (
                            select
                                symbol,
                                time_open as timestamp,
                                opening_price_in_usd,
                                highest_price_in_usd,
                                the_lowest_price_in_usd,
                                closing_price_in_usd,
                                volume_in_usd,
                                volume_in_coins,
                                volume_in_coins_when_taker_buy_coins,
                                volume_in_usd_when_taker_sell_coins,
                                transactions_per_minute
                            from raw_bn_1m_candle
                            where date between today() - 1 - 1 and today() - 1
                        ) as historical using(timestamp, symbol)
                order by symbol, timestamp desc
            ) as set_in
    ) features_for_final_aggrigation
group by
    target_name,
    target_value,
    target_timestamp,
    symbol
