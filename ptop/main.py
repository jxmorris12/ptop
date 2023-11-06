from time import monotonic

from . import slurm_helpers

from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer
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
        df = slurm_helpers.get_squeue_df()
        table = DataTable()
        table.add_columns(*df.columns)
        table.add_rows(df.to_numpy()[1:])
        yield table



class SlurmStats(App):
    """A Textual app to view SLURM status."""

    CSS_PATH = "css/ptop.tcss"

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
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

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark


def main():
    print("running app")
    SlurmStats().run()
    print("ran app")


if __name__ == '__main__': main()