from dagster import job, op, repository


@op
def hello_world(context):
    context.log.warning('jojo RULEZZZ')
    pass


@job
def hello_world_job():
    hello_world()


@repository
def hello_world_repository():
    return [hello_world_job]