import subprocess
from datetime import datetime
from enum import Enum
import json


def sbatch(job_command: str):
    p = subprocess.run(["sbatch", *job_command.split(" ")], capture_output=True)
    if p.returncode != 0:
        raise ValueError(p.stderr.decode("utf-8").strip())
    else:
        if p.stdout is not None:
            stdout = p.stdout.decode("utf-8").strip()
            print(stdout)
            return stdout


def scancel(job_id: int):
    subprocess.run(["scancel", str(job_id)])


def get_job_info_by_job_name(job_name: str) -> dict | None:
    p = subprocess.run(["sacct", "-n", "--json", "-j", job_name])
    if p.stdout is not None and p.stdout != b"":
        stdout = p.stdout.decode("utf-8").strip()
        if len(stdout) == 0:
            return None
        info = json.loads(stdout)
        return info
    return None


def get_job_info_by_job_id(job_id: int) -> dict | None:
    p = subprocess.run(["sacct", "-n", "--json", "-j", str(job_id)])

    if p.stdout is not None and p.stdout != b"":
        stdout = p.stdout.decode("utf-8").strip()
        if len(stdout) == 0:
            return None
        info = json.loads(stdout)
        return info
    return None
