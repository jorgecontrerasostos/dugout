import json

import dagster as dg
import pandas as pd
import statsapi

from constants import SPORTS_ID

daily_partition = dg.DailyPartitionsDefinition(start_date="2026-03-25")


@dg.asset(
    description="MLB schedule data from statsapi",
    key_prefix=["bronze"],
    kinds={"duckdb"},
    partitions_def=daily_partition,
    metadata={"partition_expr": "date"},
)
def schedule(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Retrieves a schedule of games based on the provided date or current date
    if no date parameter is provided.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.

    Returns:
        pd.DataFrame: A DataFrame containing the game schedule data.

    Raises:
        ValueError: If no schedule data is found from statsapi.
    """
    date = context.partition_key
    data = statsapi.get("schedule", {"sportId": SPORTS_ID, "date": date})

    if not data:
        raise ValueError("No schedule data found from statsapi")

    schedule_df = pd.DataFrame(data["dates"])
    schedule_df["games"] = schedule_df["games"].apply(json.dumps)
    return schedule_df
