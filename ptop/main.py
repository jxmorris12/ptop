import asyncio

from . import slurm_helpers

import pandas as pd

from textual import work
from textual.app import App, ComposeResult, RenderResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.coordinate import Coordinate
from textual.geometry import Spacing
from textual.reactive import reactive, var
from textual.widgets import DataTable, Footer, Header, Label, LoadingIndicator, Markdown, Static


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
    taken = reactive(1.0)
    total = reactive(1.0)
    active_color: str

    def _redraw(self) -> None:
        try:
            # TODO: How to do this without accessing _default?
            taken = self.taken._default
        except AttributeError:
            taken = self.taken
        
        try:
            total = self.total._default
        except AttributeError:
            total = self.total

        w1 = 100.0 * (1.0 - taken / total)
        w2 = 100.0 * taken / total

        r1, r2 = self.query(Rectangle)

        r1.styles.width = f"{w1:.2f}%"
        r2.styles.width = f"{w2:.2f}%"

        if taken == total: 
            # only one bar shows up in this case
            r1.label = ""
            r1.styles.margin = Spacing(left=0, right=0)
            r2.styles.margin = Spacing(left=1, right=0)
        else:
            # show both bars -- need padding
            r1.styles.margin = Spacing(left=1, right=0)
            r2.styles.margin = Spacing(left=1, right=0)
            r1.label = f"{total - taken:.2f}"
        r2.label = f"{taken:.2f}"

    def watch_taken(self):
        self._redraw()
    
    def watch_total(self):
        self._redraw()

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Label(self.get_label())

        yield Rectangle(
            label="1",
            color=self.get_active_color(),
            width="50%"
        )
        yield Rectangle(
            label="1",
            color="#808080",
            width="50%",
        )


class CpuDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#035096" # blue
    
    def get_label(self) -> str:
        return "CPU"

class CpuLoadDisplay(Indicator):
    def get_active_color(self) -> str:
        # return "#035096" # blue
        return "#800080" # purple

    def get_label(self) -> str:
        return "CPU Load"

class GpuDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#035096" # blue
        # return "#008000" # green

    def get_label(self) -> str:
        return "GPU"

class MemDisplay(Indicator):
    def get_active_color(self) -> str:
        return "#035096" # blue
        # return "#FF4500" # red

    def get_label(self) -> str:
        return "MEM"


class NodeStatus(Static):
    """A widget to display SLURM node status."""
    status = reactive(None)
    
    def set_status(self, status: slurm_helpers.NodeStatusInfo):
        self.status = status
    
    def watch_status(self, status: slurm_helpers.NodeStatusInfo):
        if status is None: return

        self.query_one(CpuDisplay).taken = reactive(status.cpu_taken)
        self.query_one(CpuDisplay).total = reactive(status.cpu_total)

        self.query_one(MemDisplay).taken = reactive(status.mem_taken)
        self.query_one(MemDisplay).total = reactive(status.mem_total)


        self.query_one(GpuDisplay).taken = reactive(status.gpu_taken)
        self.query_one(GpuDisplay).total = reactive(status.gpu_total)

        self.query_one(CpuLoadDisplay).taken = reactive(status.cpu_load)
        self.query_one(CpuLoadDisplay).total = reactive(100.0)

        self.query(Markdown)[0].update(f"## {status.hostname}")
        self.query(Markdown)[1].update(f'**GPU Users:** {self.status.gpu_users_str}')

        print("reset stuff...")

    def compose(self) -> ComposeResult:
        """Create child widgets of a stopwatch."""
        yield Markdown("")
        yield CpuDisplay()
        yield MemDisplay()
        yield GpuDisplay()
        yield Markdown("")
        yield CpuLoadDisplay()
        

class JobStatus(Static):
    def compose(self) -> ComposeResult:
        table = DataTable()
        # table.add_columns(*self.df.columns)
        # table.add_rows(self.df.to_numpy()[1:])
        yield table


class GpuStatus(Static):
    def compose(self) -> ComposeResult:
        table = DataTable()
        yield DataTable()



class SlurmStats(App):
    """A Textual app to view SLURM status."""

    CSS_PATH = "css/ptop.tcss"

    load_count: var[int] = var(0)

    def compute_fully_loaded(self) -> bool:
        return self.load_count == 4

    def watch_load_count(self, load_count: bool) -> None:
        fully_loaded = (load_count == 2)
        if fully_loaded:
            self.query_one(LoadingIndicator).display = False
            self.query_one(Container).display = True
        
    @work(thread=True)
    async def load_node_info(self) -> None:
        node_statuses, gpu_df = slurm_helpers.get_node_statuses()

        # Update node status info
        node_statuses = [s for s in node_statuses if s.hostname in ALL_HOSTNAMES]
        for status, node in zip(node_statuses, self.query(NodeStatus)):
            node.status = status
        self.load_count += 1

        # Update GPU status info
        data_table = self.query_one(GpuStatus).query_one(DataTable)
        if len(data_table.columns) == 0:
            # Only add columns the first time
            data_table.add_columns(*gpu_df.columns)
            data_table.add_rows(gpu_df.to_numpy())
        else:
            # Update all rows. We assume the GPUs we have on the cluster
            # won't change since ptop was first run.
            for i in range(len(gpu_df)):
                # row = data_table.get_row_at(i)
                for j, datum in enumerate(gpu_df.iloc[i].to_numpy()):
                    data_table.update_cell_at(Coordinate(1, 1), datum)

        # Sleep and call this function again.
        await asyncio.sleep(0.1)
        self.load_node_info()
    
    @work(thread=True)
    async def load_job_info(self) -> None:
        job_df = slurm_helpers.get_job_status_df()

        # TODO: make this more modular (should just set df and have these
        # automatically be computed.)
        data_table = self.query_one(JobStatus).query_one(DataTable)
        data_table.clear()

        if len(data_table.columns) == 0:
            # Only add columns the first time
            data_table.add_columns(*job_df.columns)
        data_table.add_rows(job_df.to_numpy())
        self.load_count += 1

        # Show "No jobs found" if we're not running anything.
        no_jobs_found_label = self.query("#no_jobs_found").first()
        if len(job_df) == 0:
            data_table.display = False
            no_jobs_found_label.display = True
        else:
            data_table.display = True
            no_jobs_found_label.display = False

        # Sleep and call this function again.
        await asyncio.sleep(2.0)
        self.load_job_info()

    def on_mount(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(Container).display = False

        self.load_node_info()
        self.load_job_info()

    def compose(self) -> ComposeResult:
        """Called to add widgets to the app."""
        yield Header("SLURM Status")

        yield LoadingIndicator()
        with Container():
            with Horizontal():
                statuses = [NodeStatus() for _ in ALL_HOSTNAMES]
                yield VerticalScroll(*statuses)
                yield Vertical(
                    Markdown("## Jobs"), 
                    JobStatus(), 
                    Markdown("### No jobs found.", id="no_jobs_found"), 
                    Markdown("## All GPUs"), 
                    GpuStatus(), 
                )
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark


def main(): 
    SlurmStats().run()


if __name__ == '__main__': main()