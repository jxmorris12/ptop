import io
# import os
import subprocess

import pandas as pd


def get_squeue_df() -> pd.DataFrame:
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

