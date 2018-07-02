import luigi
import luigi.server
import luigi.configuration

from multiprocessing import Process
import urllib
import os
import sys

def schedule(task_list, **kwargs):
    luigi.build(task_list, **kwargs)

def run_luigid():
    filename = os.path.realpath(sys.argv[0])
    unix_socket = os.path.abspath(filename + ".sock")
    state_path = os.path.abspath(filename + ".pickle")

    batch_job(["luigid", "--state-path", state_path, "--unix-socket", unix_socket])
    
    scheduler_url = urllib.parse.quote(unix_socket, safe="")
    return f"http+unix://{scheduler_url}"

def run_batch_jobs(socket_address):
    filename = os.path.realpath(sys.argv[0])
    batch_job([sys.executable, filename, "--scheduler-url", socket_address])

def batch_job(cmd, queue="s"):
    import subprocess
    subprocess.call(["bsub", "-env", "'all'", "-q", queue] + cmd)