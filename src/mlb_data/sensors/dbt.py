import dagster as dg

from ..assets.dbt import dbt_models
from ..jobs.bronze_partitioned import bronze_partitioned_job

dbt_job = dg.define_asset_job(
    name="dbt_job",
    selection=[dbt_models],
    description="""
        This job executes selected dbt_models belonging to the silver layer
        once their respective bronze models are materialized
        """
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    request_job=dbt_job,
    monitored_jobs=[bronze_partitioned_job],
)
def on_bronze_partitioned_success(context: dg.RunStatusSensorContext) -> dg.RunRequest:
    return dg.RunRequest(run_key=None)
