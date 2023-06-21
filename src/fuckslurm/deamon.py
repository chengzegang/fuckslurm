from typing import Iterable
from .slurm_template import SlurmJob, JobStateTyle
import threading
import time


class Deamon:
    def __init__(
        self,
        job: SlurmJob,
        resubmit_states: Iterable[JobStateTyle],
        frequency: int = 60,
        tryouts: int = 0,
    ):
        self.job = job
        self.resubmit_states = resubmit_states
        self.frequency = frequency
        self._stop: threading.Event = threading.Event()
        self.tryouts = tryouts
        self._tried: int = 0

    def _run_thread(self):
        while (
            not self._stop.is_set() and self.tryouts > 0 and self._tried < self.tryouts
        ):
            if self.job.state in self.resubmit_states:
                try:
                    self.job.submit()
                except Exception as e:
                    print(e)
                    break
                self._tried += 1
            time.sleep(self.frequency)

    def spin(self, thr: threading.Thread):
        if threading.current_thread() is threading.main_thread():
            while thr.is_alive():
                time.sleep(1)

    def start(self):
        self._stop.clear()
        thr = threading.Thread(target=self._run_thread, daemon=True)
        thr.start()
        self.spin(thr)
