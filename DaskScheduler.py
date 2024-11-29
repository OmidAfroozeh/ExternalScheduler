import distributed.scheduler
import dask.array as da
import time  # For keeping the script alive
from dask.distributed import LocalCluster, Client, Scheduler
from distributed.scheduler import WorkerState, TaskState, decide_worker
from typing import Callable, Any
import utils as utils
from distributed.protocol.serialize import *

from distributed import worker


SCHEDULER_URL = "http://127.0.0.1:5000"

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
    # Print some basic debug info to check if the function is called
    # print(f"ITS WORKINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
    # print(f"Deciding worker for task: {ts.key}")
    # print(f"All workers: {[worker.address for worker in all_workers]}")
    #
    #
    #
    #
    # payload = {"TEST": 1}
    #
    #
    # print("RES: \n")
    # print(res)
    # print("\n\n")
    # # Call the original logic for now (this is the same as Dask's original decide_worker)
    # # If you want, you can change the logic here
    # return decide_worker(ts, all_workers, valid_workers, objective)

# The main entry point of the script
if __name__ == "__main__":
    # Set up a LocalCluster (this is a basic, local cluster for testing)
    cluster = LocalCluster()
    client = Client(cluster)

    distributed.scheduler.decide_worker = custom_decide_worker

    # Print out the Dask dashboard link and client information
    print("Dask Dashboard available at:", client.dashboard_link)
    print(client)

    # Simple Dask computation: Create a large random array and perform a computation
    x = da.random.random((10000, 10000), chunks=(1000, 1000))  # Create a Dask array
    y = x + x.T  # A simple operation: adding the transpose

    # Compute the result (this will execute the task graph)
    result = y.sum().compute()

    print("The sum of the array is:", result)

    # Keep the client alive to monitor the dashboard
    print("Waiting for workers to finish...")

    # Infinite loop to keep the script running (you can manually stop it when done)

    while True:
        result = y.sum().compute()
        print("The sum of the array is:", result)
        time.sleep(20)  # Keeps the program running indefinitely
