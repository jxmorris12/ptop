from time import monotonic

from . import slurm_helpers

from textual.app import App, ComposeResult, RenderResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import DataTable, Footer, Header, Label, Markdown, Static


ALL_HOSTNAMES = [
    "rush-compute-01",
    "rush-compute-02",
    "rush-compute-03",
    "nlplarge-compute-01",
]


class Rectangle(Static):
    def __init__(self, label: str, color: str, width: str):
        super().__init__()
        self.label = label
        self.styles.background = color
        self.styles.width = width

    def render(self) -> RenderResult:
        return f" {self.label}"


class Indicator(Static):
    """A widget to display elapsed time.
    
    Kind of like a progress bar, for showing how much
    is left of something. Also has a label.
    """
    taken: float
    total: float
    active_color: str
    def __init__(
            self, 
            taken: float, 
            total: float,
        ):
        super().__init__()
        self.taken = taken
        self.total = total

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Label(self.get_label())
        w1 = 100.0 * self.taken / self.total
        w2 = 100.0 * (1.0 - self.taken / self.total)

        yield Rectangle(
            label=f"{self.total - self.taken:.2f}",
            color=self.get_active_color(),
            width=f"{w2:.2f}%"
        )
        yield Rectangle(
            label=f"{self.taken:.2f}",
            color="#808080",
            width=f"{w1:.2f}%"
        )


class CpuDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#035096" # blue
    
    def get_label(self) -> str:
        return "CPU"

class CpuLoadDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#800080" # purple

    def get_label(self) -> str:
        return "CPU Load"

class GpuDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#008000" # green

    def get_label(self) -> str:
        return "GPU"

class MemDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#FF4500" # red

    def get_label(self) -> str:
        return "MEM"


class NodeStatus(Static):
    """A widget to display SLURM node status."""
    status: slurm_helpers.NodeStatusInfo
    def __init__(self, status: slurm_helpers.NodeStatusInfo):
        super().__init__()
        self.status = status

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Markdown(f"## {self.status.hostname}")
        yield CpuDisplay(self.status.cpu_taken, self.status.cpu_total)
        yield MemDisplay(self.status.mem_taken, self.status.mem_total)
        yield GpuDisplay(self.status.gpu_taken, self.status.gpu_total)
        yield Markdown(f'**GPU Users:** {self.status.gpu_users_str}')
        yield CpuLoadDisplay(self.status.cpu_load, 100.0)
        

class JobStatus(Static):

    def compose(self) -> ComposeResult:
        df = slurm_helpers.get_job_status_df()
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
        yield LoadingIndicator()
        yield Header("SLURM Status")

        node_statuses = slurm_helpers.get_node_statuses(ALL_HOSTNAMES)
        with Container():
            with Horizontal():
                yield VerticalScroll(
                    *[NodeStatus(s) for s in node_statuses], 
                    id="node_status",
                )
                yield Vertical(
                    Markdown("## Job Status"), 
                    JobStatus(), 
                    id="job_status",
                )
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark


def main():
    SlurmStats().run()


if __name__ == '__main__': main()