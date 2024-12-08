import logging
import distributed.scheduler
import dask.array as da
import time  # For keeping the script alive
from dask.distributed import LocalCluster, Client, Scheduler, performance_report
from distributed.scheduler import WorkerState, TaskState, decide_worker
from typing import Callable, Any
import utils as utils
from distributed.protocol.serialize import *

from distributed import worker

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


SCHEDULER_URL = "http://127.0.0.1:5000"
EXPERIMENT_NAME = "test_run"

# Override the decide_worker function
def custom_decide_worker(
        ts: TaskState,
        all_workers: set[WorkerState],
        valid_workers: set[WorkerState] | None,
        objective: Callable[[WorkerState], Any],
) -> WorkerState | None:


    """
    Custom logic to override Dask's original decide_worker function.
    This version prints the available workers and the chosen worker to test the override.
    """
    assert all(dts.who_has for dts in ts.dependencies)
    if ts.actor:
        candidates = all_workers.copy()
    else:
        candidates = {wws for dts in ts.dependencies for wws in dts.who_has or ()}
        candidates &= all_workers
    if valid_workers is None:
        if not candidates:
            candidates = all_workers.copy()
    else:
        candidates &= valid_workers
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.loose_restrictions:
                    return decide_worker(ts, all_workers, None, objective)

    if not candidates:
        return None
    elif len(candidates) == 1:
        return next(iter(candidates))
    else:
        # Prepare the payload with worker IDs
        worker_ids = [worker.address for worker in candidates]
        payload = {
            "task_id": ts.key,  # Task identifier
            "worker_ids": worker_ids  # List of candidate worker IDs
        }
        res = utils.send_post_request(SCHEDULER_URL + '/submit_job', payload)
        chosen_worker_id = res.get("chosen_worker") if res else None
        if chosen_worker_id:
            # Map the chosen worker ID back to a WorkerState
            for worker in candidates:
                if worker.address == chosen_worker_id:
                    return worker
        return None
    

# The main entry point of the script
if __name__ == "__main__":
    # Set up a LocalCluster (this is a basic, local cluster for testing)
    cluster = LocalCluster()
    client = Client(cluster)

    distributed.scheduler.decide_worker = custom_decide_worker

    # Print out the Dask dashboard link and client information
    print("Dask Dashboard available at:", client.dashboard_link)
    print(client)

    report_filename = f"dask-report-{EXPERIMENT_NAME}.html"

    # Use performance_report to capture profiling data
    with performance_report(filename=report_filename):
        start_time = time.time()  # Capture start time

        # Example Dask computation
        x = da.random.random((100000, 100000), chunks=(10000, 10000))
        result = x.sum().compute()

        end_time = time.time()  # Capture end time
        elapsed_time = end_time - start_time  # Calculate elapsed time

        logger.info(f"Computed result: {result}")
        logger.info(f"Time taken for computation: {elapsed_time:.4f} seconds")
        logger.info(f"Performance report saved to {report_filename}")

            
    logger.info("Dask scheduler stopped.")
    client.close()
    cluster.close()

    print("The sum of the array is:", result)
