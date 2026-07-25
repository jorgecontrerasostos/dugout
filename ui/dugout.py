
from ascii import dugout_str
from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import Footer, Header, Static

from screens.standings import StandingsScreen
from screens.wildcard import WildcardScreen

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
            Static(r"\[s] - Standings", id="bindings_hint"),
            Static(r"\[w] - Wildcard Race", id="wildcard_race_hint"),
            Static(r"\[d] - Toggle Dark Mode", id="dark_mode_hint"),
            Static("Ctrl + q - Quit", id="quit_hint"),
            id="welcome_screen",
        )

        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )


if __name__ == "__main__":
    app = Dugout()
    app.run()
