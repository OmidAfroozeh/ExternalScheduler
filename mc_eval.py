import asyncio
import subprocess
import logging
import time
import distributed
import paramiko
import uuid
import random
from queue import Queue
import threading
import socket
import psutil
import os
from dask import array as da
from dask.distributed import Scheduler as DistributedScheduler
from dask.distributed import LocalCluster, Client
from distributed.scheduler import Scheduler, WorkerState, TaskState, decide_worker
from typing import Callable, Any
from getpass import getpass
import utils as utils
import time
from dask.distributed import performance_report
import distributed.scheduler
import requests  # For HTTP requests
import matplotlib.pyplot as plt

# Import required libraries for ML
from dask_ml.datasets import make_classification
from dask_ml.linear_model import LogisticRegression
import dask_ml.xgboost as dxgb
from sklearn.metrics import accuracy_score

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = 'dsys2472'
PASSWORD = ''
DASHBOARD_PORT = 8790
NODES_AMOUNT = 2
NODE_THREADS = 8
MAINNODE_IP = "127.0.0.1"  # Set later
JOBWORKER_PORT = 8788
JOBWORKER_URL = "127.0.0.1"  # Set later
SCHEDULER_URL = f"http://0.0.0.0:5002"  # For the Flask app
CLAIM_TIME = '00:15:00'
INITIAL_LOCAL_WORKERS = 0  # Initial local workers.
WORKERS_PER_NODE = 'auto'  # Number of workers for the external nodes, set to auto to auto determine based on available cpu cores.
NODE_REQUEST_TIMEOUT = 15  # Reattempt time to receive nodes.


# Custom add_worker method
async def custom_add_worker(self, comm, address: str, **kwargs):
    # Register the worker via HTTP
    try:
        response = requests.post(
            f"{SCHEDULER_URL}/register_worker",
            json={"worker_id": address}
        )
        if response.status_code == 201:
            print(f"Worker {address} successfully registered.")
        else:
            print(f"Failed to register worker {address}: {response.json()}")
    except Exception as e:
        print(f"Error registering worker {address}: {e}")

    # Call the original add_worker method
    await Scheduler.add_worker_original(self, comm, address=address, **kwargs)


# Custom remove_worker method
async def custom_remove_worker(
        self,
        address: str,
        *,
        stimulus_id: str,
        expected: bool = False,
        close: bool = True,
):
    # Deregister the worker via HTTP
    try:
        response = requests.post(
            f"{SCHEDULER_URL}/remove_worker",
            json={"worker_id": address}
        )
        if response.status_code == 200:
            print(f"Worker {address} successfully removed.")
        # else:
        # print(f"Failed to remove worker {address}: {response.json()}")
    except Exception as e:
        print(f"Error removing worker {address}: {e}")

    # Call the original remove_worker method
    return await self.remove_worker_original(
        address=address,
        stimulus_id=stimulus_id,
        expected=expected,
        close=close,
    )


# Monkey Patching
Scheduler.add_worker_original = Scheduler.add_worker
Scheduler.add_worker = custom_add_worker

Scheduler.remove_worker_original = Scheduler.remove_worker
Scheduler.remove_worker = custom_remove_worker


# Get the IP address of the main node
def get_ip_address():
    try:
        result = subprocess.run(['hostname', '-I'], stdout=subprocess.PIPE)
        ip_address = result.stdout.decode().strip().split()[0]
        if not ip_address:
            ip_address = socket.gethostbyname(socket.gethostname())
        logger.info(f"Main node IP: {ip_address}")
        return ip_address
    except Exception as e:
        logger.error(f"Failed to get IP address: {str(e)}")
        return None


MAINNODE_IP = get_ip_address()  # Update main node IP dynamically
JOBWORKER_URL = f"tcp://{MAINNODE_IP}:{JOBWORKER_PORT}"


# Function to get reserved nodes
def get_reserved_nodes():
    result = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.error("Failed to retrieve reserved nodes.")
        return [], 0
    node_lines = result.stdout.decode().splitlines()
    reserved_nodes = set()
    for line in node_lines:
        if USER in line:
            parts = line.split()
            if len(parts) > 7:
                nodes = parts[8:]
                valid_nodes = [node for node in nodes if node.startswith("node")]
                reserved_nodes.update(valid_nodes)
    return list(reserved_nodes), len(reserved_nodes)


# Function to check and reserve resources
def check_and_reserve_resources():
    reserved_nodes, total_reserved_nodes = get_reserved_nodes()
    logger.info(f"Currently {total_reserved_nodes} nodes reserved. Trying to reserve more...")
    nodes_needed = NODES_AMOUNT - total_reserved_nodes
    subprocess.run(['preserve', '-1', '-#', str(nodes_needed), '-t', CLAIM_TIME])
    while total_reserved_nodes < NODES_AMOUNT:
        reserved_nodes, total_reserved_nodes = get_reserved_nodes()
        if (total_reserved_nodes > NODES_AMOUNT):
            logger.info(f"Sufficient nodes reserved. We have {len(reserved_nodes)} nodes.")
        else:
            logger.info(f"Not sufficient nodes reserved, trying again in {NODE_REQUEST_TIMEOUT} seconds.")
        time.sleep(NODE_REQUEST_TIMEOUT)
    return reserved_nodes


# Function to get node IP
def get_node_ip(node_name):
    result = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode == 0:
        node_lines = result.stdout.decode().splitlines()
        for line in node_lines:
            if node_name in line:
                parts = line.split()
                ip_addresses = parts[1:]
                if ip_addresses:
                    return ip_addresses[0]
    return None


def get_password(node_name):
    global PASSWORD
    if PASSWORD == '':
        PASSWORD = getpass(f"Enter the password for {node_name} connection: ")
    return PASSWORD


# Function to start Dask worker on an external node via SSH
def start_ssh_worker(node_name, ip_address):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        logger.info(f"Attempting to start worker node at {node_name} to scheduler at {JOBWORKER_URL}")
        ssh.connect(node_name, username=USER, password=get_password(node_name))

        # Command to start the Dask worker
        command = f"nohup dask-worker {JOBWORKER_URL} --nthreads {NODE_THREADS} --nworkers {WORKERS_PER_NODE} > dask-worker.log 2>&1 &"

        # Execute the command
        stdin, stdout, stderr = ssh.exec_command(command)

        # Log the output and error
        stdout_output = stdout.read().decode('utf-8')
        stderr_output = stderr.read().decode('utf-8')

        if stdout_output:
            logger.info(f"STDOUT from {node_name}:\n{stdout_output}")
        if stderr_output:
            logger.error(f"STDERR from {node_name}:\n{stderr_output}")

        ssh.close()
    except Exception as e:
        logger.error(f"Failed to connect or execute command on {node_name}: {e}")
        return False
    return True


# Custom worker selection logic
# Custom decide_worker method (no changes needed from your provided code)
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
    # print("EXECUTING: custom_decide_worker()")
    assert all(dts.who_has for dts in ts.dependencies)

    # Determine the candidates
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
        # Prepare worker information for the payload

        workers_info = [
            {
                "address": worker.address,
                "memory_limit": worker.memory_limit,
                "memory_used": worker.nbytes,
                "cpu_cores": worker.nthreads,
                "tasks_running": len(worker.processing),
                "bandwidth": worker.bandwidth,
                "time_delay": worker.time_delay
                # "total_resources": dict(worker.resources),
                # "used_resources": dict(worker.used_resources)
            }
            for worker in candidates
        ]

        payload = {
            "task_id": ts.key,
            "workers": workers_info,
        }

        # Communicate with the external scheduler
        try:
            response = requests.post(f"{SCHEDULER_URL}/submit_job", json=payload)
            if response.status_code == 201:
                chosen_worker_id = response.json().get("chosen_worker")
                for worker in candidates:
                    if worker.address == chosen_worker_id:
                        return worker
        except Exception as e:
            print(f"Error communicating with external scheduler: {e}")
        return None


async def monte_carlo_evaluation(client, start_points, max_points, step, report_filename_prefix="MC-Performance"):
    """
    Run Monte Carlo simulations for increasing workloads and log execution times.

    :param client: Dask client instance
    :param start_points: Initial number of points
    :param max_points: Maximum number of points
    :param step: Step size for increasing the number of points
    :param report_filename_prefix: Prefix for performance report filenames
    """
    workloads = []
    execution_times = []

    for n_points in range(start_points, max_points + 1, step):
        logger.info(f"Starting Monte Carlo simulation for {n_points} points...")

        # Generate a unique filename for the performance report
        report_filename = f"/reports/{report_filename_prefix}-{n_points}.html"

        # Use performance_report to capture profiling data
        with performance_report(filename=report_filename):
            start_time = time.time()  # Capture start time

            # Generate random points and compute Pi estimate
            x = da.random.random(n_points, chunks=n_points // 10)
            y = da.random.random(n_points, chunks=n_points // 10)
            inside_circle = ((x ** 2 + y ** 2) <= 1).sum()
            pi_estimate = (inside_circle / n_points) * 4
            result = pi_estimate.compute()  # Trigger computation

            end_time = time.time()  # Capture end time
            execution_time = end_time - start_time

            logger.info(f"Estimated value of Pi: {result:.6f}")
            logger.info(f"Execution time for {n_points} points: {execution_time:.2f} seconds.")

            # Record the workload and execution time
            workloads.append(n_points)
            execution_times.append(execution_time)

    return workloads, execution_times


# Main function to set up scheduler and workers
async def main():
    logger.info("Setting up the Dask scheduler and workers...")

    # Step 1: Setup Dask client
    cluster = await distributed.LocalCluster(n_workers=INITIAL_LOCAL_WORKERS, host="0.0.0.0",
                                             dashboard_address=f"0.0.0.0:{DASHBOARD_PORT}",
                                             scheduler_port=JOBWORKER_PORT)
    client = await distributed.Client(cluster)
    logger.info(f"Dask Dashboard available at: {client.dashboard_link}")
    client.scheduler_info()

    # Step 2: Reserve and start external workers
    reserved_nodes = check_and_reserve_resources()
    for node in reserved_nodes:
        node_ip = get_node_ip(node)
        if node_ip:
            start_ssh_worker(node, node_ip)

    # Step 3: Start Flask app (SchedulerService)
    # def is_flask_app_running():
    #     for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
    #         try:
    #             if 'python' in proc.info['name'] and 'SchedulerService/server.py' in ' '.join(proc.info['cmdline']):
    #                 logger.info(f"Flask app already running with PID: {proc.info['pid']}")
    #                 return True
    #         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
    #             pass
    #     return False

    # if not is_flask_app_running():
    #     os.system("python SchedulerService/server.py &")
    #     logger.info("Flask app started in background.")

    # Step 4: Override Dask's worker selection logic
    distributed.scheduler.decide_worker = custom_decide_worker

    time.sleep(10)
    # Step 5: Simple Dask computation with performance_report
    try:

        # Step 2: Run Monte Carlo evaluations
        start_points = 10 ** 6  # Start with 1 million points
        max_points = 10 ** 9  # Go up to 100 million points
        step = 10 ** 7  # Increment by 1 million points
        workloads, execution_times = await monte_carlo_evaluation(client, start_points, max_points, step)
        # Step 3: Plot results
        plt.figure(figsize=(10, 6))
        plt.plot(workloads, execution_times, marker='o', linestyle='-', color='b', label='Execution Time')
        plt.title('Monte Carlo Simulation: Workload vs Execution Time')
        plt.xlabel('Number of Points (Workload)')
        plt.ylabel('Execution Time (seconds)')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()

        # Save and show the plot
        plt.savefig("workload_vs_execution_time.png")
        plt.show()
        # while True:
        #     # Generate a unique filename for the performance report
        #     report_filename = f"ML-Eval-{int(time.time())}.html"

        #     # Use performance_report to capture profiling data
        #     with performance_report(filename=report_filename):
        #         start_time = time.time()  # Capture start time

        #         # Option 1: Monte Carlo Simulation for Pi Estimation
        #         logger.info("Starting Monte Carlo simulation for Pi estimation...")
        #         n_points = 10**9  # Number of points
        #         x = da.random.random(n_points, chunks=10**6)
        #         y = da.random.random(n_points, chunks=10**6)

        #         # Count points inside the unit circle
        #         inside_circle = ((x**2 + y**2) <= 1).sum()

        #         # Estimate Pi
        #         pi_estimate = (inside_circle / n_points) * 4
        #         logger.info(f"Estimated value of Pi: {pi_estimate.compute():.6f}")
        #         end_time = time.time()  # Capture end time
        #         logger.info(f"Computation completed in {end_time - start_time:.2f} seconds.")

        #     time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Dask scheduler stopped.")
        await client.close()
        await cluster.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
