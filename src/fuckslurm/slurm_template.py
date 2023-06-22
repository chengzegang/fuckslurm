from dataclasses import dataclass, field, fields
from typing import List
import uuid
from datetime import datetime
from pathlib import Path
from . import utils
from enum import Enum


class JobStateTyle(Enum):
    COMPLETED = "COMPLETED"
    RUNNING = "RUNNING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    NONE = "NONE"


@dataclass
class Timestamp:
    senconds: int = field(default=0)
    microseconds: int = field(default=0)


@dataclass
class ExitCode:
    status: str = field(default="")
    return_code: int = field(default=0)


class Flag(Enum):
    CLEAR_SCHEDULING = "CLEAR_SCHEDULING"
    STARTED_ON_BACKFILL = "STARTED_ON_BACKFILL"


@dataclass
class JobTime:
    elapsed: int = field(default=0)
    eligible: int = field(default=0)
    end: int = field(default=0)
    start: int = field(default=0)
    submission: int = field(default=0)
    suspended: int = field(default=0)
    system: Timestamp = field(default_factory=Timestamp)
    limit: int = field(default=0)
    total: Timestamp = field(default_factory=Timestamp)
    user: Timestamp = field(default_factory=Timestamp)


@dataclass
class JobState:
    current: JobStateTyle = field(default=JobStateTyle.NONE)
    reason: str = field(default="")


@dataclass
class TRES:
    "Trackable Resource"
    type: str = field(default="")
    name: str | None = field(default=None)
    id: int = field(default=-1)
    count: int = field(default=0)


@dataclass
class JobTRES:
    requested: List[TRES] = field(default_factory=list)
    allocated: List[TRES] = field(default_factory=list)

    def __post_init__(self):
        self.requested = [TRES(**t) for t in self.requested]
        self.allocated = [TRES(**t) for t in self.allocated]


@dataclass(init=False)
class JobAssociation:
    account: str = field(default="")
    cluster: str = field(default="")
    partition: str | None = field(default=None)
    user: str = field(default="")


@dataclass(init=False)
class JobInfo:
    comment: dict = field(default_factory=dict)
    association: JobAssociation = field(default_factory=JobAssociation)
    job_id: int = field(default=-1)
    name: str = field(default="")
    time: JobTime = field(default_factory=JobTime)
    exit_code: ExitCode = field(default_factory=ExitCode)
    flags: list[Flag] = field(default_factory=list)
    group: str = field(default="")
    nodes: str = field(default="")
    partition: str = field(default="")
    priority: int = field(default=0)
    qos: str = field(default="")
    kill_request_user: str | None = field(default=None)
    reservation: dict = field(default_factory=dict)
    state: JobState = field(default_factory=JobState)
    tres: JobTRES = field(default_factory=JobTRES)

    def __init__(self, **kwargs):
        names = set([f.name for f in fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


@dataclass
class JobSubmission:
    name: str = field(default_factory=lambda: f"{uuid.uuid4().hex}-{datetime.now()}")
    out: str = field(default_factory=lambda: Path.cwd().as_posix())
    nodes: int = field(default=1)
    ntasks_per_node: int = field(default=1)
    gres: str = field(default="")
    mem: str = field(default="32GB")
    cpus_per_task: int = field(default=4)
    time: str = field(default="4:00:00")
    wrap: str = field(default="")

    def parse(self):
        return f"""--job-name={self.name} --output={self.out} --nodes={self.nodes} --ntasks-per-node={self.ntasks_per_node} --gres={self.gres} --mem={self.mem} --cpus-per-task={self.cpus_per_task} --time={self.time} --wrap={self.wrap}
                """

    def submit(self):
        sbatch_script = self.parse()
        utils.sbatch(sbatch_script)


@dataclass
class SlurmJob:
    submission: JobSubmission = field(default_factory=JobSubmission)

    @property
    def jobinfo(self) -> JobInfo:
        jobs = utils.get_job_info_by_job_name(self.submission.name)
        if jobs is None:
            raise ValueError("No job found.")
        return JobInfo(**jobs["jobs"][0])

    @property
    def scheduled(self) -> bool:
        jobs = utils.get_job_info_by_job_name(self.submission.name)
        return jobs is not None

    def submit(self):
        self.submission.submit()

    def cancel(self):
        utils.scancel(self.jobinfo.job_id)

    @property
    def state(self) -> JobStateTyle:
        if not self.scheduled:
            return JobStateTyle.NONE
        return self.jobinfo.state.current

    @property
    def name(self) -> str:
        return self.submission.name

    @property
    def job_id(self) -> int:
        if not self.scheduled:
            return -1
        return self.jobinfo.job_id
