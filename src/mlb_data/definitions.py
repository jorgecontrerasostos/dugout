import dagster as dg
from dagster_dbt import DbtCliResource

from .assets.teams import teams
from .assets.standings import standings
from .assets.schedule import schedule
from .assets.game_feed import game_feed
from .assets.player_stats import player_stats
from .assets.dbt import dbt_models

from .jobs.bronze_partitioned import bronze_partitioned_job, bronze_partitioned_schedule
from .jobs.bronze_snapshot import bronze_snapshot_schedule, bronze_snapshot_job

from .sensors.dbt import on_bronze_partitioned_success, dbt_job

from .resources.duckdb import duckdb_io_manager


defs = dg.Definitions(
    assets=[teams, schedule, standings, game_feed, player_stats, dbt_models],
    resources={
        "io_manager": duckdb_io_manager,
        "dbt": DbtCliResource(project_dir="dbt/"),
    },
    jobs=[bronze_partitioned_job, bronze_snapshot_job, dbt_job],
    schedules=[bronze_partitioned_schedule, bronze_snapshot_schedule],
    sensors=[on_bronze_partitioned_success],
)
