import dagster as dg

from ..assets.teams import teams
from ..assets.standings import standings

bronze_snapshot_job = dg.define_asset_job(
    name="bronze_snapshot_job",
    selection=[teams, standings],
    description="This job is triggers the materialization of assets listed.",
)

bronze_snapshot_schedule = dg.ScheduleDefinition(
    name="bronze_snapshot_schedule",
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    job=bronze_snapshot_job,
)
