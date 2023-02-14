insert into default.prc_bn_1m_candle_set
select
    part_name,
    target_price_in_in_usd,
    round(arrayFilter
        (
            x -> x != 0,
            arrayMap
                (
                    (l, a, h, i) ->
                        multiIf
                            (
                                l = 0 or a = 0 or a = 0, 0,
                                l <= any(target_price_in_in_usd * 0.5), -50,
                                h >= target_price_in_in_usd * (1.001 + 1/100), 1, /* stop gain pct */
                                i + 1 = length(groupArrayIf(avg_price_in_usd, interval_id=2)),
                                    ((a * 0.999 * 100) / target_price_in_in_usd) - 100,
                                0
                            ),
                    groupArrayIf(the_lowest_price_in_usd, interval_id=2),
                    groupArrayIf(avg_price_in_usd, interval_id=2),
                    groupArrayIf((highest_price_in_usd + avg_price_in_usd)/2, interval_id=2),
                    range(length(groupArrayIf(avg_price_in_usd, interval_id=2)))

                )
        )[1], 3) as gain_1_pct,
    floor(maxIf
        (
            ((avg_price_in_usd * 0.999 * 100) / target_price_in_in_usd) - 100,
            interval_id = 2
        )) as chery_max_avg_price_in_usd,
    target_name,
    target_value,
    target_timestamp,
    target_symbol,
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
        ) as feature_sum_of_changes_in_backlist_for_avg_prices_changed_per_1minute_pct,
    floor(sumIf(volume_in_usd, interval_id in (0, -1)) / countIf(interval_id in (0, -1))) as avg_vol_per_min
from
    (
        with
            if(volume_in_usd*volume_in_coins != 0, volume_in_usd/volume_in_coins, 0) as avg_price_in_usd,
            if(target_id - neighbor(target_id, 1) != 0, 0, 1) as is_last_row_in_target,
            floor
                (
                    1000 * if
                        (
                            is_last_row_in_target * neighbor(avg_price_in_usd, 1) != 0,
                            ((is_last_row_in_target * avg_price_in_usd * 100) / neighbor(avg_price_in_usd, 1)) - 100,
                            0
                        )
                ) / 1000 as feature_avg_prices_changed_per_1minute_pct
        select
            toDate(target_timestamp) as part_name,

            symbol as target_symbol,
            target_name,
            target_timestamp,

            timestamp,

            interval_id,

            target_value,
            price_in as target_price_in_in_usd,

            the_lowest_price_in_usd,
            avg_price_in_usd,
            highest_price_in_usd,
            volume_in_usd,
            volume_in_coins_when_taker_buy_coins,
            volume_in_usd_when_taker_sell_coins,
            transactions_per_minute,

            feature_avg_prices_changed_per_1minute_pct
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
                            murmurHash2_32(time_open, symbol, target_name) as target_id,
                            time_open as target_timestamp,
                            symbol,
                            arrayJoin
                                (
                                    arrayMap
                                        (
                                            x -> time_open - interval x-60 minute, /* кол-во минут после покупки */
                                            range(5*60) /* кол-во минут в кейсе */
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
                            and toDate(time_open) = toDate('{part_name}')
                            and target_name in
                                (
                                    'price_increase_1',
                                    'price_increase_2',
                                    'price_increase_3',
                                    'price_increase_7'
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
                            where date between toDate('{part_name}') - 1 and toDate('{part_name}')
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
                            where date between toDate('{part_name}') - 1 and toDate('{part_name}')
                        ) as price_in on
                            price_in.target_timestamp = targets.target_timestamp and
                            price_in.symbol = targets.symbol
                where price_in.price_in != 0
                        and volume_in_coins != 0
                        and volume_in_usd != 0
                order by targets.symbol, targets.target_name, targets.target_timestamp, targets.timestamp asc
            ) as set_in
        order by symbol, target_name, target_timestamp, timestamp asc
    ) features_for_final_aggrigation
group by
    part_name,
    target_symbol,
    target_name,
    target_timestamp,
    target_value,
    target_price_in_in_usd
having avg_vol_per_min >= 50000
order by target_symbol, target_name, target_timestamp