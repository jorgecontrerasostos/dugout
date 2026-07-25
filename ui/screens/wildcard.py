from textual.widgets import Header, Static, Footer, DataTable, Label
from textual.containers import Container, Vertical
from textual.app import App, ComposeResult
from textual.screen import Screen

from collections import defaultdict
import duckdb as dd


class WildcardScreen(Screen):
    BINDINGS = [("b", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Wildcard Race", id="wildcard_screen_title")
        yield Container(id="wildcard_container")
        yield Footer()

    def on_mount(self):

        groups = defaultdict(list)

        wildcard_container = self.get_widget_by_id("wildcard_container", Container)

        with dd.connect("./db/mlb.duckdb") as con:
            wildcard = con.sql("SELECT * FROM gold.gold_wildcard").fetchall()
            cols = [
                "team name",
                "wins",
                "losses",
                "pct",
                "wcgb",
                "home split",
                "away split",
                "run differential",
                "streak",
            ]
            for item in wildcard:
                if item[5] == 0:
                    section = "leader"
                else:
                    section = "wildcard"

                groups[item[0], section].append(item)

            current_league = None
            for group in sorted(groups):
                wildcard_individual_container = Vertical()
                wildcard_table = DataTable(
                    id=group[0].lower().replace(" ", "") + "_" + group[1].lower()
                )
                label_text = (
                    f"{group[0]} Leaders"
                    if group[1] == "leader"
                    else f"{group[0]} Wildcard"
                )

                label_color = "#D50032" if "National League" in group[0] else "#002D72"

                wildcard_table_title = Label(
                    f"[bold {label_color}]{label_text}[/bold {label_color}]"
                )

                if current_league != group[0] and current_league is not None:
                    wildcard_container.mount(Static("-" * self.size.width))

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
                current_league = group[0]

