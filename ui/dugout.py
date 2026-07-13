from collections import defaultdict

from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, DataTable, Label, Static
from textual.containers import Container, Vertical, Horizontal
from textual.screen import Screen


from ascii import dugout_str

import duckdb as dd


class WildcardScreen(Screen):
    BINDINGS = [("b", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Wildcard screen")
        yield Container(id="wildcard_container")
        yield Footer()

    def on_mount(self):

        groups = defaultdict(list)

        wildcard_container = self.get_widget_by_id("wildcard_container", Container)

        with dd.connect("./db/mlb.duckdb") as con:
            wildcard = con.sql("SELECT * FROM gold.gold_wildcard").fetchall()
            cols = [
                "team_name",
                "wins",
                "losses",
                "pct",
                "wild_card_games_back",
                "home_split",
                "away_split",
                "run_differential",
                "streak",
            ]
            for item in wildcard:
                if item[5] == 0:
                    section = "leader"
                else:
                    section = "wildcard"

                groups[item[0], section].append(item)

            for group in sorted(groups):
                wildcard_individual_container = Vertical()
                wildcard_table = DataTable(id=group[0].lower().replace(" ", "") + "_" + group[1].lower())
                wildcard_table_title = Label(
                    group[0] + " Leaders" if group[1] == "leader" else group[0] + " Wildcard"
                )

                wildcard_container.mount(wildcard_individual_container)
                wildcard_individual_container.mount(wildcard_table_title)
                wildcard_individual_container.mount(wildcard_table)

                wildcard_table.add_columns(*cols)
                transformed_rows = []
                for row in groups[group]:
                    if row[5] == 0.0:
                        transformed_rows.append(row[:5] + ("-",) + row[6:])
                    else:
                        transformed_rows.append(row)
                wildcard_table.add_rows(row[1:] for row in transformed_rows)


class StandingsScreen(Screen):
    BINDINGS = [("b", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(id="standings_container")
        yield Footer()

    def on_mount(self):
        # defaultdict type. it's values are going to be a list
        groups = defaultdict(list)

        # accessing standings container by its id
        standings_container = self.get_widget_by_id("standings_container", Container)

        # connecting to duckdb file using with so they connection is closed automatically
        with dd.connect("./db/mlb.duckdb") as con:
            standings = con.sql("SELECT * FROM gold.gold_standings").fetchall()
            cols = ["team", "wins", "losses", "pct", "games back"]

            for item in standings:
                groups[(item[0], item[1])].append(item)
            """
            groups is a dictionary in which the key is a tuple made of the league and division
            and the values is a list of tuples containing the whole row values
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
                standing_title = Label(group[1])

                standings_container.mount(standing_individual_container)
                standing_individual_container.mount(standing_title)
                standing_individual_container.mount(standings_table)

                standings_table.add_columns(*cols)
                standings_table.add_rows(row[2:] for row in groups[group])


class Dugout(App):
    """Dugout is a TUI (Terminal User Interface) that displays MLB Data."""

    BINDINGS = [
        ("d", "toggle_dark", "Toggle Dark Mode"),
        ("s", "push_screen('standings')", "Open Standings Screen"),
        ("w", "push_screen('wildcard')", "Open Wildcard Screen"),
    ]
    CSS_PATH = "dugout.tcss"
    SCREENS = {"standings": StandingsScreen, "wildcard": WildcardScreen}

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""

        yield Header()
        yield Vertical(
            Static(dugout_str, id="dugout_title"),
            Static("\[s] - Standings", id="bindings"),
            Static("\[w] - Wildcard Race"),
            Static("\[d] - Toggle Dark Mode"),
            Static("Ctrl + q - Quit"),
            id="welcome_screen")

        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )


if __name__ == "__main__":
    app = Dugout()
    app.run()
