from textual.widgets import Header, Static, Footer, DataTable, Label
from textual.containers import Container, Vertical
from textual.app import App, ComposeResult
from textual.screen import Screen

from collections import defaultdict
import duckdb as dd

class StandingsScreen(Screen):
    BINDINGS = [("b", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("[bold]Standings[/bold]", id="standings_screen_title")
        yield Container(id="standings_container")
        yield Footer()

    def on_mount(self):
        # defaultdict type. it's values are going to be a list
        groups = defaultdict(list)

        # accessing standings container by its id
        standings_container = self.get_widget_by_id("standings_container", Container)

        # connecting to duckdb file using with so the connection
        # is closed automatically
        with dd.connect("./db/mlb.duckdb") as con:
            standings = con.sql("SELECT * FROM gold.gold_standings").fetchall()
            cols = ["team", "wins", "losses", "pct", "games back"]

            for item in standings:
                groups[(item[0], item[1])].append(item)
            """
            groups is a dictionary in which the key is a tuple made of the
            league and division and the values is a list of tuples
            containing the whole row values
            {
            ("American League", "AL East"): [
                ("American League", "AL East", "New York Yankees", 50, 30, 0.625, 0.0),
                ("American League", "AL East", "Baltimore Orioles", 45, 35, 0.562, 5.0)
                ]
            }
            """
            for group in groups:
                standing_individual_container = Vertical()
                standings_table = DataTable(id=group[1].lower().replace(" ", ""))
                standing_title = Label(
                    f"[bold #D50032]{group[1]}[/bold #D50032]"
                    if "National League" in group[0]
                    else f"[bold #002D72]{group[1]}[/bold #002D72]"
                )

                standings_container.mount(standing_individual_container)
                standing_individual_container.mount(standing_title)
                standing_individual_container.mount(standings_table)

                standings_table.add_columns(*cols)
                standings_table.add_rows(row[2:] for row in groups[group])
