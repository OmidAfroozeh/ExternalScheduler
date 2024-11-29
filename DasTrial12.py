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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = 'dsys2470'
PASSWORD = ''
SCHEDULER_IP = "127.0.0.1"
SCHEDULER_PORT = 8788
DASHBOARD_PORT = 8790
NODES_AMOUNT = 4
NODE_THREADS = 4
SCHEDULER_URL = f"http://{SCHEDULER_IP}:5000"  # For the Flask app

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

SCHEDULER_IP = get_ip_address()  # Update main node IP dynamically
SCHEDULER_URL = f"tcp://{SCHEDULER_IP}:{SCHEDULER_PORT}"

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
        subprocess.run(['preserve', '-1', '-#', str(nodes_needed), '-t', '00:15:00'])
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
        logger.info(f"Attempting to start worker node at {node_name} to scheduler at {SCHEDULER_URL}")
        ssh.connect(node_name, username=USER, password=get_password(node_name))
        
        # Command to start the Dask worker
        command = f"nohup dask-worker {SCHEDULER_URL} --nthreads {NODE_THREADS} > dask-worker.log 2>&1 &"
        
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
    objective: Callable[[WorkerState], Any]
) -> WorkerState | None:
    assert all(dts.who_has for dts in ts.dependencies)
    candidates = {wws for dts in ts.dependencies for wws in dts.who_has or ()} & all_workers
    if not candidates and valid_workers:
        candidates = valid_workers
    if not candidates:
        return None
    worker_ids = [worker.address for worker in candidates]
    payload = {"task_id": ts.key, "worker_ids": worker_ids}
    res = utils.send_post_request(SCHEDULER_URL + '/submit_job', payload)
    chosen_worker_id = res.get("chosen_worker") if res else None
    return next((worker for worker in candidates if worker.address == chosen_worker_id), None)

# Main function to set up scheduler and workers
async def main():
    logger.info("Setting up the Dask scheduler and workers...")

    # Step 1: Setup Dask client
    cluster = await distributed.LocalCluster(host="0.0.0.0", dashboard_address=f"0.0.0.0:{DASHBOARD_PORT}", scheduler_port=SCHEDULER_PORT)
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
    distributed.scheduler.decide_worker = custom_decide_worker

    # Step 5: Simple Dask computation
    try:
        while True:
            x = da.random.random((1000, 1000), chunks=(100, 100))
            result = x.mean().compute()
            logger.info(f"Computed result: {result}")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Dask scheduler stopped.")
        await client.close()
        await cluster.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
