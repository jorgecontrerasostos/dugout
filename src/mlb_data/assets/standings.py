import json

import dagster as dg
import pandas as pd
import statsapi


@dg.asset(
    description="MLB standings data from statsapi",
    key_prefix=["bronze"],
    kinds={"duckdb"},
)
def standings() -> pd.DataFrame:
    standings = statsapi.get("standings", {"leagueId": "103,104"})
    if not standings:
        raise ValueError("No standings data found from statsapi")
    standings_df = pd.DataFrame(standings["records"])
    # Convert the teamRecords column to a valid JSON string
    # so it can be stored in the database
    standings_df["teamRecords"] = standings_df["teamRecords"].apply(json.dumps)
    return standings_df
