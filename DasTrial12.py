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
from distributed.scheduler import WorkerState, TaskState, decide_worker
from typing import Callable, Any
from getpass import getpass
import utils as utils
import time
from dask.distributed import performance_report


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = os.getlogin()
PASSWORD = ''
DASHBOARD_PORT = 8790
NODES_AMOUNT = 4
NODE_THREADS = 16
MAINNODE_IP = "127.0.0.1"  #Set later
JOBWORKER_PORT = 8788
JOBWORKER_URL = "127.0.0.1" #Set later
SCHEDULER_URL = f"http://0.0.0.0:5000" # For the Flask app
CLAIM_TIME = '00:15:00'
INITIAL_LOCAL_WORKERS = 0 # Initial local workers.
WORKERS_PER_NODE = 'auto' # Number of workers for the external nodes, set to auto to auto determine based on available cpu cores.
EXPERIMENT_NAME = "test_run"

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
        logger.info(f"Reserved nodes amount: {total_reserved_nodes}, wanted nodes: {NODES_AMOUNT}, Check status valid: {total_reserved_nodes > NODES_AMOUNT}")
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
    print("EXECUTING: custom_decide_worker()")
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

# Main function to set up scheduler and workers
async def main():
    logger.info("Setting up the Dask scheduler and workers...")

    # Step 1: Setup Dask client
    cluster = await distributed.LocalCluster(n_workers = INITIAL_LOCAL_WORKERS, host="0.0.0.0", dashboard_address=f"0.0.0.0:{DASHBOARD_PORT}", scheduler_port=JOBWORKER_PORT)
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
    def is_flask_app_running():
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if 'python' in proc.info['name'] and 'SchedulerService/server.py' in ' '.join(proc.info['cmdline']):
                    logger.info(f"Flask app already running with PID: {proc.info['pid']}")
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False

    if not is_flask_app_running():
        os.system("python SchedulerService/server.py &")
        logger.info("Flask app started in background.")

    # Step 4: Override Dask's worker selection logic
    # distributed.scheduler.decide_worker = custom_decide_worker

    # Step 5: Simple Dask computation with performance_report
    report_filename = f"dask-report-{EXPERIMENT_NAME}.html"

    # Use performance_report to capture profiling data
    with performance_report(filename=report_filename):
        start_time = time.time()  # Capture start time

        # Example Dask computation
        x = da.random.random((100000, 100000), chunks=(10000, 10000))
        result = x.mean().compute()

        end_time = time.time()  # Capture end time
        elapsed_time = end_time - start_time  # Calculate elapsed time

        logger.info(f"Computed result: {result}")
        logger.info(f"Time taken for computation: {elapsed_time:.4f} seconds")
        logger.info(f"Performance report saved to {report_filename}")

            
    logger.info("Dask scheduler stopped.")
    await client.close()
    await cluster.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
