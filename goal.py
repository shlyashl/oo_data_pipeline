import importlib
import sys
from datetime import timedelta
from dotenv import load_dotenv
import pkg_resources
from time import sleep

from pyaml_env import parse_config


from pipline.tools import *

sys.path.extend(['.'])
sys.path.extend(['projects'])
sys.path.extend(['projects/packages'])
sys.path.extend(['projects/packages/binance-historical-market-data'])
get_bn_history = importlib.import_module('binance-historical-market-data.main').main



load_dotenv(pkg_resources.resource_filename(__name__, '.env'))
config = parse_config(pkg_resources.resource_filename(__name__, 'config.yml'))

import pandas as pd



ch = ClickHouse(**config['clickhouse'], table='prc_bn_1m_candle_set')
for execution_date in pd.date_range(start="2017-08-18",end="2023-01-24"):
    ed = execution_date.strftime('%Y-%m-%d')
    print(ed)
    queries = [
        ch.queries['insert_prc_bn_1m_candle_targets'].format(
            part_name=ed, 
            past_avg_price_minute_start=growtt_interval_in_minutes, 
            past_avg_price_minute_end=growtt_interval_in_minutes
        ) 
        for growtt_interval_in_minutes in range(1, 21)
    ]

    ch.drop_part(ed)

    for i, q in enumerate(queries):
        logger.info(f'{i} starting insert query for {ed=}')
        ch.run_query(q)
