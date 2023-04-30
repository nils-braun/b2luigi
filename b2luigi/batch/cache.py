import abc
from cachetools import TTLCache


class BatchJobStatusCache(abc.ABC, TTLCache):
    """
    Abstract base class for job status caches.
    Useful if the batch system provides the status of all jobs
    as a list, which might be faster than asking for each job
    separately.

    Override the function _ask_for_job_status, which should
    set the job status for the specific job if
    specified or for all accessible jobs (e.g. for all of this user).
    Having too much information (e.g. information on jobs
    which are not started by this b2luigi instance) does not matter.
    """
    def __init__(self):
        super().__init__(maxsize=1000, ttl=120)

    @abc.abstractmethod
    def _ask_for_job_status(self, job_id=None):
        pass

    def __missing__(self, job_id):
        # First, ask for all jobs
        self._ask_for_job_status(job_id=None)
        if job_id in self:
            return self[job_id]

        # Then, ask specifically for this job
        self._ask_for_job_status(job_id=job_id)
        if job_id in self:
            return self[job_id]

        raise KeyError
