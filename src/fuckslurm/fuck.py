from typing import List
from .slurm_template import SlurmJob, JobSubmission, JobStateTyle
from .deamon import Deamon
from typer import Typer
import yaml

app = Typer()


@app.command()
def fuck(
    name: str,
    out: str,
    nodes: int,
    ntasks_per_node: int,
    gres: str,
    mem: str,
    cpus_per_task: int,
    time: str,
    wrap: str,
    resubmit_states: List[JobStateTyle] = [
        JobStateTyle.FAILED,
        JobStateTyle.TIMEOUT,
    ],
    frequency: int = 60,
    tryouts: int = 10,
):
    job = SlurmJob(
        JobSubmission(
            name=name,
            out=out,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            gres=gres,
            mem=mem,
            cpus_per_task=cpus_per_task,
            time=time,
            wrap=wrap,
        )
    )
    daemon = Deamon(job, resubmit_states, frequency, tryouts)
    daemon.start()


@app.command()
def fuck_as_planned(plan: str):
    details: dict = yaml.load(open(plan, "rb"), yaml.FullLoader)
    resubmit_states = details.pop("resubmit_states")
    resubmit_states = [JobStateTyle(s) for s in resubmit_states]
    frequency = details.pop("frequency")
    tryouts = details.pop("tryouts")
    job = SlurmJob(JobSubmission(**details))
    daemon = Deamon(job, resubmit_states, frequency, tryouts)
    daemon.start()
