from time import monotonic

from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Label, Markdown, ProgressBar, Static


class NodeStatusDisplay(Static):
    """A widget to display elapsed time."""

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Label(self.get_label())
        yield ProgressBar()


class CpuDisplay(NodeStatusDisplay):
    def get_label(self) -> str:
        return "CPU"

class GpuDisplay(NodeStatusDisplay):
    def get_label(self) -> str:
        return "GPU"

class MemDisplay(NodeStatusDisplay):
    def get_label(self) -> str:
        return "MEM"


class NodeStatus(Static):
    """A widget to display SLURM node status."""
    hostname: str
    def __init__(self, hostname: str):
        super().__init__()
        self.hostname = hostname

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Markdown(f"## {self.hostname}")
        yield CpuDisplay()
        yield MemDisplay()
        yield GpuDisplay()

class JobStatus(Static):

    def compose(self) -> ComposeResult:
        ROWS = [
            ("lane", "swimmer", "country", "time"),
            (4, "Joseph Schooling", "Singapore", 50.39),
            (2, "Michael Phelps", "United States", 51.14),
            (5, "Chad le Clos", "South Africa", 51.14),
            (6, "László Cseh", "Hungary", 51.14),
            (3, "Li Zhuhao", "China", 51.26),
            (8, "Mehdy Metella", "France", 51.58),
            (7, "Tom Shields", "United States", 51.73),
            (1, "Aleksandr Sadovnikov", "Russia", 51.84),
            (10, "Darren Burns", "Scotland", 51.84),
        ]
        table = DataTable()
        table.add_columns(*ROWS[0])
        table.add_rows(ROWS[1:])
        yield table



class PtopApp(App):
    """A Textual app to manage stopwatches."""

    CSS_PATH = "css/ptop.tcss"

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("a", "add_stopwatch", "Add"),
        ("r", "remove_stopwatch", "Remove"),
    ]

    def compose(self) -> ComposeResult:
        """Called to add widgets to the app."""
        hostnames = [
            *[f"rush-compute-0{idx}" for idx in range(1,4)],
            "nlp-large-01",
        ]
        yield Header("SLURM Node Status")
        yield Footer()
        yield ScrollableContainer(*[NodeStatus(host) for host in hostnames], id="timers")
        yield ScrollableContainer(JobStatus(), id="timers2")

    def action_add_stopwatch(self) -> None:
        """An action to add a timer."""
        new_stopwatch = Stopwatch()
        self.query_one("#timers").mount(new_stopwatch)
        new_stopwatch.scroll_visible()

    def action_remove_stopwatch(self) -> None:
        """Called to remove a timer."""
        timers = self.query("Stopwatch")
        if timers:
            timers.last().remove()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark


def main():
    print("running app")
    PtopApp().run()
    print("ran app")


if __name__ == '__main__': main()