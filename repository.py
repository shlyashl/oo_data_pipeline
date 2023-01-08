import importlib
import sys
from datetime import timedelta

from dagster import job, op, repository
from dagster import schedule, OpExecutionContext, ScheduleEvaluationContext, RunRequest

sys.path.extend(['projects/packages'])

# binance-historical-market-data
sys.path.extend(['projects/packages/binance-historical-market-data'])
get_bn_history = importlib.import_module('binance-historical-market-data.main').main


@op
def update_bn_history_data(context: OpExecutionContext):
    ed = context.get_tag('date')
    get_bn_history(ed, ed)

@job
def update_bn_history_data_job():
    update_bn_history_data()

@schedule(
    job=update_bn_history_data_job, cron_schedule='5 0 * * *', execution_timezone='Europe/Moscow'
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
