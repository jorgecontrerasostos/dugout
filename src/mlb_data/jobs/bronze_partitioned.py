from datetime import datetime, timedelta

import dagster as dg

from ..assets.game_feed import game_feed
from ..assets.player_stats import player_stats
from ..assets.schedule import schedule


def _target_partition_key(date: datetime) -> str:
    return (date - timedelta(days=1)).strftime("%Y-%m-%d")


bronze_partitioned_job = dg.define_asset_job(
    name="bronze_partitioned_job",
    selection=[schedule, game_feed, player_stats],
    description="This job is triggers the materialization of assets listed.",
)


@dg.schedule(
    job=bronze_partitioned_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def bronze_partitioned_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _target_partition_key(context.scheduled_execution_time)
    return dg.RunRequest(partition_key=partition_key)
