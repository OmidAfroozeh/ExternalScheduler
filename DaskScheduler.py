import distributed.scheduler
import dask.array as da
import time  # For keeping the script alive
from dask.distributed import LocalCluster, Client, Scheduler
from distributed.scheduler import WorkerState, TaskState, decide_worker
from typing import Callable, Any
import utils as utils

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
    # Print some basic debug info to check if the function is called
    print(f"ITS WORKINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
    print(f"Deciding worker for task: {ts.key}")
    print(f"All workers: {[worker.address for worker in all_workers]}")

    payload = {"TEST": 1}

    res = utils.send_post_request(SCHEDULER_URL + '/submit_job', payload)

    print("RES: \n")
    print(res)
    print("\n\n")
    # Call the original logic for now (this is the same as Dask's original decide_worker)
    # If you want, you can change the logic here
    return decide_worker(ts, all_workers, valid_workers, objective)

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