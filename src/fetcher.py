import logging
import sqlite3
import pathlib

DB_PATH = pathlib.Path(__file__).parent / "data" / "mlb_stats.db"


def create_db():
    try:
        conn = sqlite3.connect(DB_PATH)
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
            team TEXT,
            position TEXT,
            season INTEGER,
            games_played INTEGER,
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
            team TEXT,
            position TEXT,
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
        conn.close()
    except sqlite3.OperationalError:
        logging.log(1, "Could not create DB")


def main():
    create_db()


if __name__ == "__main__":
    main()
