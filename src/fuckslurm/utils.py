import subprocess
from datetime import datetime
from enum import Enum
import json


def sbatch(job_command: str):
    subprocess.run(["sbatch", job_command], shell=True, check=True)


def scancel(job_id: int):
    subprocess.run(["scancel", str(job_id)], check=True)


def get_job_info_by_job_name(job_name: str) -> dict:
    outs = subprocess.run(
        ["sacct", "-n", "--json", "-j", job_name], capture_output=True
    )
    info = json.loads(outs.stdout.decode("utf-8").strip())
    return info


def get_job_info_by_job_id(job_id: int) -> dict:
    outs = subprocess.run(
        ["sacct", "-n", "--json", "-j", str(job_id)], capture_output=True
    )
    info = json.loads(outs.stdout.decode("utf-8").strip())
    return info
