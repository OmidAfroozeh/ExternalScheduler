import asyncio
import subprocess
import logging
import time
import distributed
import paramiko
from getpass import getpass
import uuid
import random
from queue import Queue
import threading
import socket
from dask.distributed import Client, Scheduler, Worker, LocalCluster
from dask import array as da
import os  # Import os for running background processes
from dask.distributed import Scheduler as DistributedScheduler

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = 'dsys2470'
PASSWORD = ''
SCHEDULER_IP = "127.0.0.1"  # Default to localhost for Dask
SCHEDULER_PORT = 8786  # Default port for Dask scheduler
NODES_AMOUNT = 4

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
        ssh.connect(node_name, username=USER, password=get_password(node_name))
        command = f"dask-worker {SCHEDULER_IP}:{SCHEDULER_PORT} --nthreads 1 --memory-limit 2GB"
        ssh.exec_command(command)
        ssh.close()
    except Exception as e:
        logger.error(f"Failed to connect to {node_name}: {e}")
        return False
    return True

# Custom decide_worker to send the task info to the external scheduler
def custom_decide_worker(
        ts, all_workers, valid_workers, objective
):
    """
    Custom logic to override Dask's original decide_worker function.
    """
    if valid_workers is None:
        valid_workers = all_workers

    candidates = valid_workers
    if not candidates:
        return None
    elif len(candidates) == 1:
        return next(iter(candidates))
    else:
        # Send request to external scheduler if necessary
        worker_ids = [worker.address for worker in candidates]
        payload = {"task_id": ts.key, "worker_ids": worker_ids}
        # Add logic to fetch the chosen worker from external scheduler if available
        return next(iter(candidates))

# Main function to set up scheduler and workers
async def main():
    logger.info("Setting up the Dask scheduler and workers...")

    # Step 1: Set up the Dask Scheduler (default)
    scheduler = DistributedScheduler(host='0.0.0.0', port='8786')
    await scheduler.start()

    # Step 2: Reserve and start external workers
    reserved_nodes = check_and_reserve_resources()
    for node in reserved_nodes:
        node_ip = get_node_ip(node)
        if node_ip:
            start_ssh_worker(node, node_ip)

    logger.info(f"Dask Scheduler is running at: {SCHEDULER_IP}:{SCHEDULER_PORT}")
    
    # Step 3: Setup Dask client to interact with scheduler
    cluster = LocalCluster(dashboard_address="0.0.0.0:8788")
    client = Client(cluster)
    print("Dask Dashboard available at:", client.dashboard_link)

    client.scheduler_info()
    
    # Set custom decide_worker for task allocation
    distributed.scheduler.decide_worker = custom_decide_worker

    logger.info("Dask Scheduler setup completed.")
    
    # Step 4: Start simple Dask computation in the main thread
    try:
        while True:
            x = da.random.random((1000, 1000), chunks=(100, 100))
            result = x.mean().compute()  # Compute the mean
            logger.info(f"Computed result: {result}")
            time.sleep(10)  # Sleep to prevent overloading the CPU (or change based on actual needs)
    except KeyboardInterrupt:
        logger.info("Dask scheduler stopped.")
        scheduler.close()

    # Step 5: Start the external SchedulerService (Flask app) **last**
    try:
        logger.info("Starting external scheduler service (Flask app)...")
        os.system("python SchedulerService/server.py &")  # Flask app started in background
        logger.info("Flask app started in background.")
    except Exception as e:
        logger.error(f"Failed to start Flask app: {e}")
        
if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())