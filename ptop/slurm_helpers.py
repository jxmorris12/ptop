from typing import Dict, List

import collections
import dataclasses
import io
import re
import shlex
import subprocess

import pandas as pd


def get_job_status_df() -> pd.DataFrame:
    """Runs squeue which produces output like below and parses into a pandas dataframe.
    
                 JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
           1123860       gpu 06112023     jxm3  R      51:53      1 lil-compute-04
           1123861       gpu 06112023     jxm3  R      51:53      1 lil-compute-04
           1123862       gpu 06112023     jxm3  R    2:53:41      1 badfellow
           1123863       gpu 06112023     jxm3  R    2:53:41      1 badfellow
           1123859       gpu 06112023     jxm3  R    1:14:11      1 joachims-compute-03
           1123864       gpu 06112023     jxm3  R    1:14:11      1 seo-compute-01
           1123857      rush 06112023     jxm3 PD       0:00      1 (Priority)
           1123856      rush 06112023     jxm3 PD       0:00      1 (Resources)
           1123852      rush 06112023     jxm3  R    1:03:16      1 rush-compute-03
           1123853      rush 06112023     jxm3  R    1:03:16      1 rush-compute-03
           1123854      rush 06112023     jxm3  R    1:03:16      1 rush-compute-03
           1123855      rush 06112023     jxm3  R    1:03:16      1 rush-compute-03
           1124203 rush-inte      zsh     jxm3  R      28:46      1 rush-compute-01
    """
    cmd_output = subprocess.check_output(["squeue"])
    # breakpoint()
    output_file = io.StringIO(cmd_output.decode())
    return pd.read_table(output_file, delim_whitespace=True, skiprows=1)


def get_node_info_df() -> pd.DataFrame:
    """See SLURM node info/stats."""
    node_info_output = subprocess.check_output(
        'sinfo -N -O "NodeList:30,Partition:30,CPUsState:30,CPUsLoad:30,Memory:30,FreeMem:30,StateCompact:30,Threads:30,Gres:30"',
        shell=True, universal_newlines=True
    )
    return pd.read_table(
        io.StringIO(node_info_output), delim_whitespace=True,
    )


def get_all_jobs_df() -> pd.DataFrame():
    """See all running jobs on SLURM."""
    # Custom parsing for this part because the output disagrees with pandas.
    running_jobs_output = subprocess.check_output(
        "sacct --format=User%10,partition%20,NodeList%25,State%10,AllocTRES%50,Time -a --units=G",
        shell=True, universal_newlines=True
    )
    str_rows = running_jobs_output.split('\n')
    rows = []
    columns = None
    for i, row in enumerate(str_rows):
        values = [
            row[:10].strip(),
            row[11:31].strip(),
            row[32:57].strip(),
            row[58:68].strip(),
            row[69:119].strip(),
            row[120:].strip(),
        ]
        if i == 0:
            columns = values
        elif i == 1: 
            continue
        else:
            rows.append(values)
    return pd.DataFrame(rows, columns=columns)


@dataclasses.dataclass
class NodeStatusInfo:
    hostname: str
    ###############
    gpu_taken: int
    gpu_total: int
    ###############
    cpu_load: float
    ###############
    cpu_taken: int
    cpu_total: int
    ###############
    mem_taken: int
    mem_total: int
    ###############
    gpu_users: Dict[str, int]

    @property
    def gpu_users_str(self) -> str:
        user_strs = [
            f'{user} ({num_gpus})' for user, num_gpus in sorted(self.gpu_users.items(), key=lambda item: item[1])
        ]
        return ', '.join(user_strs)


def get_node_statuses(hostnames: List[str]) -> List[NodeStatusInfo]:
    node_info_df = get_node_info_df()   
    jobs_df = get_all_jobs_df()

    # Filter to only running jobs.
    jobs_df["is_billing"] = (
        jobs_df["AllocTRES"].map(lambda s: s.startswith("billing="))
        &
        jobs_df["Timelimit"].map(lambda s: len(s) > 0)
    )
    jobs_df = jobs_df[
        (jobs_df["is_billing"]) & (jobs_df["State"] == "RUNNING")]

    # Filter to only info about hosts we care about.
    node_info_df["is_local_host"] = node_info_df["NODELIST"].map(lambda n: n in hostnames)
    jobs_df["is_local_host"] = jobs_df["NodeList"].map(lambda n: n in hostnames)

    node_info_df = node_info_df.drop(["PARTITION"], axis=1).drop_duplicates().reset_index()
    node_info_df = node_info_df[node_info_df["is_local_host"]].reset_index()
    jobs_df = jobs_df[jobs_df["is_local_host"]].reset_index()
    
    # Create status by combining node info and job info.
    for h in hostnames:
        try:
            info = node_info_df[node_info_df["NODELIST"] == h].iloc[0]
        except IndexError:
            print(f"Warning: no info found for host {h}")
            continue
        jobs = jobs_df[jobs_df["NodeList"] == h]

        # Get CPU info.
        # CPUS(A/I/O/T) => Allocated / Idle / Other / Total
        try:
            cpu_taken, cpu_idle, cpu_other, cpu_total = map(float, info["CPUS(A/I/O/T)"].split("/"))
        except ValueError:
            print("<>> INFO:", info["CPUS(A/I/O/T)"]    )
            cpu_taken = cpu_total = float(info["CPUS(A/I/O/T)"])
        mem_taken = int(info["MEMORY"]) - int(info["FREE_MEM"])
        mem_total = int(info["MEMORY"])

        # Count GPUs.
        #    gpu:titanrtx:8(S:0-1) -> 8
        total_gpus = int(re.search(r"gpu:\w+:(\d)\(.+", info["GRES"]).group(1))

        # Now subtract in-use GPUs from each job.
        taken_gpus_by_user = collections.defaultdict(lambda: 0)
        for _, job in jobs.iterrows():
            user = job["User"]
            # billing=4,cpu=4,gres/gpu:titanrtx=1,gres/gpu=1,me+ => 1
            try:
                gpus_str = re.search(r"gres/gpu=(\d+)", job["AllocTRES"]).group(1)
            except AttributeError:
                gpus_str = "0"
            # Track GPUs in use (even if it's zero)
            taken_gpus_by_user[user] += int(gpus_str)

        taken_gpus = sum(taken_gpus_by_user.values())

        # Aggregate all info into one object.
        node_status = NodeStatusInfo(
            hostname=h,
            ###############
            gpu_taken=taken_gpus,
            gpu_total=total_gpus,
            ###############
            cpu_load=float(info["CPU_LOAD"]),
            cpu_taken=cpu_taken,
            cpu_total=cpu_total,
            ###############
            mem_taken=mem_taken,
            mem_total=mem_total,
            ###############
            gpu_users=taken_gpus_by_user,
        )
        yield node_status


if __name__ == "__main__":
    print(
        list(
            get_node_statuses([
                "rush-compute-01",
                "rush-compute-02",
                "rush-compute-03",
                "nlplarge-compute-01",
            ])
        )
    )