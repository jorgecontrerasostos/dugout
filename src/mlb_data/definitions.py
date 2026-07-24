import dagster as dg
from dagster_dbt import DbtCliResource

from .assets.dbt import dbt_models
from .assets.game_feed import game_feed
from .assets.player_stats import player_stats
from .assets.schedule import schedule
from .assets.standings import standings
from .assets.teams import teams
from .jobs.bronze_partitioned import bronze_partitioned_job, bronze_partitioned_schedule
from .jobs.bronze_snapshot import bronze_snapshot_job, bronze_snapshot_schedule
from .resources.duckdb import duckdb_io_manager
from .sensors.dbt import dbt_job, on_bronze_partitioned_success

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
