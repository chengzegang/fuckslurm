import subprocess
from datetime import datetime
from enum import Enum
import json


def sbatch(job_command: str):
    p = subprocess.Popen(["sbatch", *job_command.split(" ")])
    p.wait()
    if p.returncode != 0:
        raise ValueError(
            f"sbatch failed. Please check your job command. \n stderr: {p.stderr}"
        )


def scancel(job_id: int):
    subprocess.Popen(["scancel", str(job_id)])


def get_job_info_by_job_name(job_name: str) -> dict | None:
    p = subprocess.Popen(["sacct", "-n", "--json", "-j", job_name])
    p.wait()
    if p.stdout is not None:
        stdout = p.stdout.read().decode("utf-8").strip()
        if len(stdout) == 0:
            return None
        info = json.loads(stdout)
        return info
    return None


def get_job_info_by_job_id(job_id: int) -> dict | None:
    p = subprocess.Popen(["sacct", "-n", "--json", "-j", str(job_id)])
    p.wait()
    if p.stdout is not None:
        stdout = p.stdout.read().decode("utf-8").strip()
        if len(stdout) == 0:
            return None
        info = json.loads(stdout)
        return info
    return None
