select sum(gain_1_pct) ttl_gain_pct,
       sum(gain_1_pct)/count() avg_gain_pct,
       count() ttl_ins,
       countIf(gain_1_pct > 0) / count() as win_pct,
       target_name
from (

select
    price_in,
    round(arrayFilter
        (
            x -> x != 0,
            arrayMap
                (
                    (l, a, h, i) ->
                        multiIf
                            (
                                l = 0 or a = 0 or a = 0, 0,
                                l <= any(price_in * 0.5), -50,
                                h >= price_in * (1.001 + 1/100), 1, /* stop gain pct */
                                i + 1 = length(groupArrayIf(avg_price, interval_id=2)), ((l * 0.999 * 100) / price_in) - 100,
                                0
                            ),
                    groupArrayIf(the_lowest_price_in_usd, interval_id=2),
                    groupArrayIf(avg_price, interval_id=2),
                    groupArrayIf(avg_between_highest_price_and_avg_price_in_usd, interval_id=2),
                    range(length(groupArrayIf(avg_price, interval_id=2)))

                )
        )[1], 3) as gain_1_pct,
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
            price_in,
            target_name,
            target_value,
            target_timestamp,
            interval_id,
            symbol,
            timestamp,
            feature_avg_prices_changed_per_1minute_pct,
            the_lowest_price_in_usd,
            (highest_price_in_usd + avg_price)/2 as avg_between_highest_price_and_avg_price_in_usd,
            avg_price
        from
            (
                select
                    targets.target_name as target_name,
                    targets.target_value as target_value,
                    targets.target_id as target_id,
                    targets.target_timestamp as target_timestamp,
                    targets.symbol as symbol,
                    targets.timestamp as timestamp,
                    targets.interval_id as interval_id,
                    historical.opening_price_in_usd as opening_price_in_usd,
                    historical.highest_price_in_usd as highest_price_in_usd,
                    historical.the_lowest_price_in_usd as the_lowest_price_in_usd,
                    historical.closing_price_in_usd as closing_price_in_usd,
                    historical.volume_in_usd as volume_in_usd,
                    historical.volume_in_coins as volume_in_coins,
                    historical.volume_in_coins_when_taker_buy_coins as volume_in_coins_when_taker_buy_coins,
                    historical.volume_in_usd_when_taker_sell_coins as volume_in_usd_when_taker_sell_coins,
                    historical.transactions_per_minute as transactions_per_minute,
                    price_in.price_in as price_in
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
                                            x -> time_open - interval x-60 minute,
                                            range(5*60)
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
--                             and target_name = 'price_increase_3'
                            /* and (symbol, time_open) in
                                (
                                    ('ACHUSDT', '2023-02-02 02:18:00'),
                                    ('ASTRUSDT', '2023-02-02 02:38:00')
                                ) */
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
                        ) as historical on
                            historical.timestamp = targets.timestamp and
                            historical.symbol = targets.symbol
                    any left join
                        (
                            select
                                symbol, time_open,
                                time_open - interval 1 minute as target_timestamp,
                                if(volume_in_usd*volume_in_coins != 0, volume_in_usd/volume_in_coins, 0)*1.001 as price_in
                            from raw_bn_1m_candle
                            where date between today() - 1 - 1 and today() - 1
                        ) as price_in on
                            price_in.target_timestamp = targets.target_timestamp and
                            price_in.symbol = targets.symbol
                order by targets.symbol, targets.timestamp desc





            ) as set_in
        order by symbol, timestamp desc
    ) features_for_final_aggrigation
group by
    target_name,
    target_value,
    target_timestamp,
    symbol,
    price_in
) group by target_name order by toUInt32OrZero(splitByChar('_', target_name)[3]) asc
