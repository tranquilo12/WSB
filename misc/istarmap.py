# istarmap.py for Python 3.8+
import multiprocessing.pool as mpp


def istarmap(self, func, iterable, chunk_size=1):
    """starmap-version of imap
    """
    self._check_running()
    if chunk_size < 1:
        raise ValueError("Chunk size must be 1+, not {0:n}".format(chunk_size))

    task_batches = mpp.Pool._get_tasks(func, iterable, chunk_size)
    result = mpp.IMapIterator(self)
    self._taskqueue.put(
        (
            self._guarded_task_generation(result._job, mpp.starmapstar, task_batches),
            result._set_length,
        )
    )
    return (item for chunk in result for item in chunk)

mpp.Pool.istarmap = istarmap