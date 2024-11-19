import distributed.scheduler
import dask.array as da
import time  # For keeping the script alive
from dask.distributed import LocalCluster, Client, Scheduler
from distributed.scheduler import WorkerState, TaskState, decide_worker
from typing import Callable, Any
import utils as utils
import dill;

SCHEDULER_URL = "http://127.0.0.1:5000"

from typing import Any, Dict
import itertools
import weakref

import base64
from typing import Dict, Any


def taskStateTransformer(ts: TaskState) -> Dict[str, Any]:
    """
    Transforms a TaskState object into a JSON-serializable dictionary,
    ensuring that byte-like data is properly encoded.

    Args:
        ts (TaskState): The TaskState object to transform.

    Returns:
        Dict[str, Any]: A dictionary representation of the TaskState.
    """

    def serialize_set_of_states(state_set: set) -> list:
        """Converts a set of TaskState (or similar) objects to a list of their keys."""
        return [state.key for state in state_set] if state_set else []

    def encode_bytes(byte_data: bytes) -> str:
        """Encodes byte data into a base64 string for JSON serialization."""
        if isinstance(byte_data, bytes):
            return base64.b64encode(byte_data).decode('utf-8')
        return byte_data  # If it's not bytes, return as is

    # Constructing the dictionary
    return {
        'key': ts.key,
        'run_spec': ts.run_spec,  # can be None, or a dict-like structure depending on the type
        'state': str(ts.state),  # Convert TaskStateState to a string (enum or similar)
        'priority': ts.priority,
        'dependencies': serialize_set_of_states(ts.dependencies),
        'dependents': serialize_set_of_states(ts.dependents),
        'has_lost_dependencies': ts.has_lost_dependencies,
        'waiting_on': serialize_set_of_states(ts.waiting_on),
        'waiters': serialize_set_of_states(ts.waiters),
        'who_wants': [client.key for client in ts.who_wants] if ts.who_wants else [],
        'who_has': [worker.key for worker in ts.who_has] if ts.who_has else [],
        'processing_on': ts.processing_on.key if ts.processing_on else None,
        'retries': ts.retries,
        'nbytes': ts.nbytes if isinstance(ts.nbytes, int) else encode_bytes(ts.nbytes),  # Handle byte data
        'type': ts.type,
        'exception': encode_bytes(ts.exception),  # Ensure exception is serialized correctly
        'traceback': encode_bytes(ts.traceback),  # Ensure traceback is serialized correctly
        'exception_text': ts.exception_text,
        'traceback_text': ts.traceback_text,
        'exception_blame': ts.exception_blame.key if ts.exception_blame else None,
        'erred_on': list(ts.erred_on) if ts.erred_on else [],
        'suspicious': ts.suspicious,
        'host_restrictions': list(ts.host_restrictions) if ts.host_restrictions else [],
        'worker_restrictions': list(ts.worker_restrictions) if ts.worker_restrictions else [],
        'resource_restrictions': ts.resource_restrictions,
        'loose_restrictions': ts.loose_restrictions,
        'actor': ts.actor,
        'group': ts.group.name if ts.group else None,  # Assuming group has a name or identifier
        'metadata': ts.metadata,
        'annotations': ts.annotations,
        'run_id': ts.run_id,
        'queueable': ts._queueable,
        'hash': ts._hash,
    }


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
    print(f"Starting monkey patch")
    print(f"Deciding worker for task: {ts.key}")
    print(f"All workers: {[worker.address for worker in all_workers]}")

    print(ts)
    payload = dill.dumps(taskStateTransformer(ts))

    print(payload)

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