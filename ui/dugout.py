from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, DataTable

import duckdb as dd

class Dugout(App):
    """Dugout is a TUI (Terminal User Interface) that displays MLB Data."""

    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield DataTable(id="standings_table")
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )

    def on_mount(self):
        with (dd.connect("./db/mlb.duckdb")) as con:
            standings = con.sql("SELECT * FROM gold.gold_standings").fetchall()
            standings_table = self.get_widget_by_id(
                "standings_table",
                DataTable
            )
            cols = ["league", "division", "team", "wins", "losses", "pct", "games back"]
            standings_table.add_columns(*cols)
            standings_table.add_rows(standings)


if __name__ == "__main__":
    app = Dugout()
    app.run()