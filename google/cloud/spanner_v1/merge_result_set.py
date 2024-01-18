from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import Any, TYPE_CHECKING
from threading import Lock

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import BatchSnapshot

QUEUE_SIZE_PER_WORKER = 32
LOCK = Lock()


def _set_metadata(merged_result_set, results):
    LOCK.acquire()
    try:
        merged_result_set._metadata = results.metadata
        merged_result_set._stats = results.stats
        merged_result_set._first_row_read = True
    finally:
        LOCK.release()


class PartitionExecutor:
    """
    Executor that executes single partition on a separate thread and inserts
    rows in the queue
    """

    def __init__(self, batch_snapshot, partition_id, merged_result_set):
        self._batch_snapshot: BatchSnapshot = batch_snapshot
        self._partition_id = partition_id
        self._merged_result_set: MergedResultSet = merged_result_set
        self._queue: Queue[PartitionExecutorResult] = merged_result_set._queue

    def run(self):
        try:
            results = self._batch_snapshot.process_query_batch(self._partition_id)
            merged_result_set = self._merged_result_set
            for row in results:
                if not merged_result_set._first_row_read:
                    _set_metadata(merged_result_set, results)
                self._queue.put(PartitionExecutorResult(data=row))
            # Special case: The result set did not return any rows.
            # Push the metadata to the merged result set.
            if not merged_result_set._first_row_read:
                _set_metadata(merged_result_set, results)
        except Exception as ex:
            self._queue.put(PartitionExecutorResult(exception=ex))
        finally:
            self._queue.put(PartitionExecutorResult(is_last=True))


@dataclass
class PartitionExecutorResult:
    data: Any = None
    exception: Exception = None
    is_last: bool = False


class MergedResultSet:
    """
    Executes multiple partitions on different threads and then combines the
    results from multiple queries using a synchronized queue. The order of the
    records in the MergedResultSet is not guaranteed.
    """

    def __init__(self, batch_snapshot, partition_ids, max_parallelism):
        self._metadata = None
        self._stats = None
        partition_ids_count = len(partition_ids)
        self._finished_counter = partition_ids_count
        # True if at least one row has been read from any of the partition
        self._first_row_read = False
        if max_parallelism != 0:
            parallelism = min(partition_ids_count, max_parallelism)
        else:
            parallelism = partition_ids_count
        self._queue = Queue(maxsize=QUEUE_SIZE_PER_WORKER * parallelism)

        partition_executors = []
        for partition_id in partition_ids:
            partition_executors.append(
                PartitionExecutor(batch_snapshot, partition_id, self)
            )
        executor = ThreadPoolExecutor(max_workers=parallelism)
        for partition_executor in partition_executors:
            executor.submit(partition_executor.run)
        executor.shutdown(False)

    def __next__(self):
        while True:
            partition_result = self._queue.get()
            if partition_result.is_last:
                self._finished_counter -= 1
                if self._finished_counter == 0:
                    raise StopIteration
            else:
                return partition_result.data

    @property
    def metadata(self):
        while True:
            if self._first_row_read:
                return self._metadata

    @property
    def stats(self):
        while True:
            if self._first_row_read:
                return self._stats

    def __iter__(self):
        return self
