import json

import dagster as dg
import pandas as pd
import statsapi

from .schedule import daily_partition


@dg.asset(
    description="MLB game feed data from statsapi",
    key_prefix=["bronze"],
    kinds={"duckdb"},
    partitions_def=daily_partition,
    metadata={"partition_expr": "date"},
)
def game_feed(
    context: dg.AssetExecutionContext, schedule: pd.DataFrame
) -> pd.DataFrame:
    date = context.partition_key
    games = json.loads(schedule["games"].iloc[0]) if not schedule.empty else []
    finished_pks = [g["gamePk"] for g in games if g["status"]["statusCode"] == "F"]

    feeds = []
    for pk in finished_pks:
        feed = statsapi.get("game", {"gamePk": pk})
        feeds.append({"gamePk": pk, "date": date, "feed": json.dumps(feed)})
    return pd.DataFrame(feeds, columns=["gamePk", "date", "feed"])
