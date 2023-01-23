insert into default.prc_bn_1m_candle_targets
select *
from (
    with calculated as (
        with
            'price_increase_{timeframe_size_in_minutes}' as target_name,
            toDate('{part_name}')                                                       as part_name,
            {past_avg_price_minute_start}                                               as past_avg_price_minute_start,
            {past_avg_price_minute_end}                                                 as past_avg_price_minute_end,
            (
                (time_open - neighbor(time_open, -5*60))/60 = 300
                and symbol = neighbor(symbol, -5*60)
            )                                                                           as is_all_rows_exists_during_5h,
            is_all_rows_exists_during_5h * (volume_in_usd / volume_in_coins)            as avg_price_in_usd,

            1000 *
            (
                avg_price_in_usd /
                (
                    is_all_rows_exists_during_5h *
                    (
                        sum(volume_in_usd) over
                            (
                                partition by symbol
                                order by time_open asc
                                rows between toUInt16(past_avg_price_minute_end) preceding
                                    and toUInt16(past_avg_price_minute_start) preceding
                            ) /
                        sum(volume_in_coins) over
                            (
                                partition by symbol
                                order by time_open asc
                                rows between toUInt16(past_avg_price_minute_end) preceding
                                    and toUInt16(past_avg_price_minute_start) preceding
                            )
                    )
                )
            )                                                                           as target_value_tmp
        select
            target_name,
            symbol,
            part_name,
            time_open,
            accurateCastOrNull(floor(target_value_tmp), 'Int64') / 1000                 as target_value
        from default.raw_bn_1m_candle
        where date between part_name - 1 and part_name
    )
    select target_name,
           symbol,
           part_name,
           time_open,
           max(target_value)                                                            as value
    from calculated
    group by symbol, part_name, time_open, target_name
    order by symbol, time_open
    limit 1 by symbol, part_name, time_open, target_name
) as freezed_tbl
where part_name = toDate(time_open) and value >= 1.01
order by symbol, toStartOfTenMinutes(time_open)
limit 1 by symbol, toStartOfTenMinutes(time_open)