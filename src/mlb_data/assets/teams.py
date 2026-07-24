import dagster as dg
import pandas as pd
import statsapi

from constants import SPORTS_ID


@dg.asset(
    description="MLB teams data from statsapi", key_prefix=["bronze"], kinds={"duckdb"}
)
def teams() -> pd.DataFrame:
    teams = statsapi.get("teams", {"sportId": SPORTS_ID})
    if not teams:
        raise ValueError("No teams data found from statsapi")
    return pd.DataFrame(teams["teams"])
