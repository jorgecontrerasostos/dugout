import logging
import sqlite3
import pathlib
import statsapi
from rich import print

DB_PATH = pathlib.Path(__file__).parent.parent / "data" / "mlb_stats.db"

def get_connection(db_path: pathlib.Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def create_tables(conn: sqlite3.Connection):
    try:
        cursor = conn.cursor()
        standings_table = """
        CREATE TABLE IF NOT EXISTS standings (
            team_id INTEGER,
            team_name TEXT,
            division TEXT,
            season INTEGER,
            wins INTEGER,
            losses INTEGER,
            games_back TEXT,
            div_rank TEXT,
            league_rank TEXT,
            wildcard_rank TEXT,
            wildcard_gb TEXT,
            PRIMARY KEY (team_id, season)
        )
        """
        batting_stats_table = """
        CREATE TABLE IF NOT EXISTS batting_stats (
            player_id INTEGER,
            player_name TEXT,
            age INTEGER,
            team TEXT,
            position TEXT,
            pos_abbreviation TEXT,
            season INTEGER,
            games_played INTEGER,
            plate_appearances INTEGER,
            at_bats INTEGER,
            hits INTEGER,
            doubles INTEGER,
            triples INTEGER,
            home_runs INTEGER,
            rbi INTEGER,
            runs INTEGER,
            stolen_bases INTEGER,
            strike_outs INTEGER,
            base_on_balls INTEGER,
            avg REAL,
            obp REAL,
            slg REAL,
            ops REAL,
            PRIMARY KEY (player_id, season)
        )
        """
        pitching_stats_table = """
        CREATE TABLE IF NOT EXISTS pitching_stats (
            player_id INTEGER,
            player_name TEXT,
            age INTEGER,
            team TEXT,
            position TEXT,
            pos_abbreviation TEXT,
            season INTEGER,
            games_played INTEGER,
            games_started INTEGER,
            wins INTEGER,
            losses INTEGER,
            saves INTEGER,
            innings_pitched REAL,
            strike_outs INTEGER,
            base_on_balls INTEGER,
            era REAL,
            whip REAL,
            strikeouts_per_9_innings REAL,
            walks_per_9_innings REAL,
            homeruns_per_9_innings REAL,
            PRIMARY KEY (player_id, season)
        )
        """
        cursor.execute(standings_table)
        cursor.execute(batting_stats_table)
        cursor.execute(pitching_stats_table)

        conn.commit()
    except sqlite3.OperationalError:
        logging.error("Could not create DB")
        raise


def fetch_standings_data(conn: sqlite3.Connection, season: int) -> None:
    data = statsapi.standings_data(season=season)
    for _, data in data.items():
        division = data.get("div_name", "No division found")
        teams = data.get("teams", "No teams found")
        for team in teams:
            team_id = team.get("team_id", "No team_id found")
            team_name = team.get("name", "No team_name found")
            wins = team.get("w", "No wins found")
            losses = team.get("l", "No losses found")
            games_back = team.get("gb", "No games back found")
            div_rank = team.get("div_rank", "No division rank found")
            league_rank = team.get("league_rank", "No league rank found")
            wildcard_rank = team.get("wc_rank", "No wild card rank found")

            insert_query = """
              INSERT OR REPLACE INTO standings                                   
                  (team_id, team_name, division, season, wins, losses,           
              games_back, div_rank, league_rank, wildcard_rank)
              VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = (
                team_id,
                team_name,
                division,
                season,
                wins,
                losses,
                games_back,
                div_rank,
                league_rank,
                wildcard_rank,
            )

            conn.execute(insert_query, values)
        conn.commit()


def fetch_batting_data(conn: sqlite3.Connection, season: int) -> None:
    teams = statsapi.lookup_team("")
    for team in teams:
        team_id = team.get("id", "No team ID found")
        full_roster = statsapi.get(
            "team_roster", {"teamId": team_id, "hydrate": "person"}
        )
        roster_data = full_roster.get("roster", "No roster found")
        for player in roster_data:
            player_id = player.get("person", "No player found").get(
                "id", "No player id"
            )
            player_name = player.get("person", "No player found").get(
                "fullName", "No player name found"
            )
            position = (
                player.get("person", "No player found")
                .get("primaryPosition", "No primary position found")
                .get("type", "No type found")
            )
            pos_abbreviation = (
                player.get("person", "No player found")
                .get("primaryPosition", "No primary position found")
                .get("abbreviation", "No type found")
            )
            if position == "Pitcher":
                continue

            player_stats = statsapi.player_stat_data(
                player_id, group="hitting", season=season
            )

            if not player_stats.get("stats"):
                continue

            team = player_stats.get("current_team", "No current team found")
            stat_line = player_stats.get("stats", "No stats found")
            stats = stat_line[0]["stats"]

            age = stats.get("age", None)
            games_played = stats.get("gamesPlayed", None)
            at_bats = stats.get("atBats", None)
            hits = stats.get("hits", None)
            doubles = stats.get("doubles", None)
            triples = stats.get("triples", None)
            home_runs = stats.get("homeRuns", None)
            rbi = stats.get("rbi", None)
            runs = stats.get("runs", None)
            stolen_bases = stats.get("stolenBases", None)
            strike_outs = stats.get("strikeOuts", None)
            base_on_balls = stats.get("baseOnBalls", None)
            avg = stats.get("avg", "No avg found")
            obp = stats.get("obp", "No obp found")
            slg = stats.get("slg", "No slg found")
            ops = stats.get("ops", "No ops found")
            plate_appearances = stats.get("plateAppearances", None)

            values = (
                player_id,
                player_name,
                age,
                team,
                position,
                pos_abbreviation,
                season,
                games_played,
                plate_appearances,
                at_bats,
                hits,
                doubles,
                triples,
                home_runs,
                rbi,
                runs,
                stolen_bases,
                strike_outs,
                base_on_balls,
                avg,
                obp,
                slg,
                ops,
            )

            insert_query = """
            INSERT OR REPLACE INTO batting_stats (
                player_id,
                player_name,
                age,
                team,
                position,
                pos_abbreviation,
                season,
                games_played,
                plate_appearances,
                at_bats,
                hits,
                doubles,
                triples,
                home_runs,
                rbi,
                runs,
                stolen_bases,
                strike_outs,
                base_on_balls,
                avg,
                obp,
                slg,
                ops
            )
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            conn.execute(insert_query, values)
        conn.commit()


def fetch_pitching_stats(conn: sqlite3.Connection, season: int) -> None:
    pass


def main():
    conn = get_connection(DB_PATH)
    create_tables(conn)
    fetch_standings_data(conn, 2025)
    fetch_batting_data(conn, 2025)

    conn.close()


if __name__ == "__main__":
    main()
