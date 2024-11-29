import socket
import subprocess
import logging
import time
import paramiko
from getpass import getpass
from dask.distributed import Client, LocalCluster
from dask import array as da
import distributed.scheduler
from distributed.scheduler import WorkerState, TaskState
from typing import Callable, Any
import utils as utils

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = 'dsys2470'
SCHEDULER_IP = "127.0.0.1"  # This will be set dynamically later
SCHEDULER_PORT = 5000
NODES_AMOUNT = 4
SCHEDULER_URL = f"http://{SCHEDULER_IP}:{SCHEDULER_PORT}"

# Get the IP address of the main node
def get_ip_address():
    try:
        result = subprocess.run(['hostname', '-I'], stdout=subprocess.PIPE)
        ip_address = result.stdout.decode().strip().split()[1]
        if not ip_address:
            ip_address = socket.gethostbyname(socket.gethostname())
        logger.info(f"Main node IP: {ip_address}")
        return ip_address
    except Exception as e:
        logger.error(f"Failed to get IP address: {str(e)}")
        return None

SCHEDULER_IP = get_ip_address()  # Update main node IP dynamically

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
    while total_reserved_nodes < NODES_AMOUNT:
        logger.info(f"Currently {total_reserved_nodes} nodes reserved. Trying to reserve more...")
        nodes_needed = NODES_AMOUNT - total_reserved_nodes
        subprocess.run(['preserve', '-1', '-#', str(nodes_needed), '-t', '00:00:30'])
        time.sleep(5)
        reserved_nodes, total_reserved_nodes = get_reserved_nodes()
    logger.info(f"Sufficient nodes reserved. We have {len(reserved_nodes)} nodes.")
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

# Function to start Dask worker
def start_ssh_worker(node_name, ip_address):
    password = getpass(f"Enter password for {node_name}: ")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(node_name, username=USER, password=password)
        command = f"dask-worker {SCHEDULER_IP}:{SCHEDULER_PORT} --nthreads 1 --memory-limit 2GB"
        ssh.exec_command(command)
        ssh.close()
    except Exception as e:
        logger.error(f"Failed to connect to {node_name}: {e}")
        return False
    return True

# Custom decide_worker logic
def custom_decide_worker(
        ts: TaskState,
        all_workers: set[WorkerState],
        valid_workers: set[WorkerState] | None,
        objective: Callable[[WorkerState], Any],
) -> WorkerState | None:
    assert all(dts.who_has for dts in ts.dependencies)
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
        worker_ids = [worker.address for worker in candidates]
        payload = {"task_id": ts.key, "worker_ids": worker_ids}
        res = utils.send_post_request(SCHEDULER_URL + '/submit_job', payload)
        chosen_worker_id = res.get("chosen_worker") if res else None
        if chosen_worker_id:
            for worker in candidates:
                if worker.address == chosen_worker_id:
                    return worker
        return None

def main():
    # Step 1: Set up a LocalCluster (basic local cluster for testing)
    logger.info("Setting up a local Dask cluster for testing...")
    cluster = LocalCluster()
    client = Client(cluster)
    logger.info(f"Dask Dashboard available at: {client.dashboard_link}")

    # Step 2: Perform continuous computation (running forever until manually canceled)
    logger.info("Running continuous test Dask computation...")

    try:
        while True:
            test_computation = client.submit(sum, range(1000000))
            logger.info(f"Test computation result: {test_computation.result()}")
            # Add a small sleep interval between computations to avoid overwhelming the scheduler
            time.sleep(10)  # Sleep for 10 seconds before running the next computation
    except KeyboardInterrupt:
        logger.info("Test computation stopped manually.")
        client.close()  # Close the Dask client connection
        logger.info("Dask client connection closed.")

if __name__ == "__main__":
    main()
