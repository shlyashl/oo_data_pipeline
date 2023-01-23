import importlib
import sys
from datetime import timedelta
from dotenv import load_dotenv
import pkg_resources

from pyaml_env import parse_config
from dagster import job, op, repository
from dagster import schedule, OpExecutionContext, ScheduleEvaluationContext, RunRequest

from projects.src import tools as t
sys.path.extend(['projects'])
sys.path.extend(['projects/packages'])
# binance-historical-market-data
sys.path.extend(['projects/packages/binance-historical-market-data'])
get_bn_history = importlib.import_module('binance-historical-market-data.main').main



load_dotenv(pkg_resources.resource_filename(__name__, '.env'))
config = parse_config(pkg_resources.resource_filename(__name__, 'config.yml'))



@op
def update_bn_history_data(context: OpExecutionContext):
    ed = context.get_tag('date')
    get_bn_history(ed, ed)

@op
def update_bn_targets_data(context: OpExecutionContext):
    ed = context.get_tag('date')
    ch = t.ClickHouse(**config['clickhouse'], table='prc_bn_1m_candle_targets')

    queries = [
        ch.queries['insert_prc_bn_1m_candle_targets'].format(
            part_name=ed, 
            past_avg_price_minute_start=growtt_interval_in_minutes, 
            past_avg_price_minute_end=growtt_interval_in_minutes
        ) 
        for growtt_interval_in_minutes in range(1, 21)
    ]

    ch.drop_part(ed)

    for i, q in enumirate(queries):
        t.logger.info(f'{i} starting insert query for {ed=}')
        ch.run_query(q)



@job
def update_bn_history_data_job():
    update_bn_history_data()
    update_bn_targets_data()



@schedule(
    job=update_bn_history_data_job, 
    cron_schedule='5 3 * * *', 
    execution_timezone='Europe/Moscow'
)
def update_bn_history_data_job_schedule(context: ScheduleEvaluationContext):
    scheduled_date = (
            context.scheduled_execution_time - timedelta(days=1)
    ).strftime('%Y-%m-%d')
    return RunRequest(
        tags={'date': scheduled_date}
    )



@repository
def update_bn_history_data_job_schedule():
    return [update_bn_history_data_job_schedule]
