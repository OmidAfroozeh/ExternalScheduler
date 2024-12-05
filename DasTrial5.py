import socket
import subprocess
import logging
import time
import paramiko
from getpass import getpass
import uuid
import random
from flask import Flask, request, jsonify
from queue import Queue
import threading
from dask.distributed import Client, Scheduler, Worker
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

# Flask app for job queue management
app = Flask(__name__)
job_queue = Queue()  # Queue to hold incoming jobs
job_status = {}  # Dictionary to track the status of jobs by job_id

# Endpoint to submit a job
@app.route('/submit_job', methods=['POST'])
def submit_job():
    job_id = str(uuid.uuid4())  # Generate a unique job ID
    job_data = request.get_json()  # Get job details from request body

    worker_ids = job_data.get("worker_ids")
    if not worker_ids or not isinstance(worker_ids, list):
        return jsonify({"error": "Invalid or missing 'worker_ids' in payload"}), 400

    # Randomly choose a worker from the provided list
    chosen_worker = random.choice(worker_ids)
    job_data["job_id"] = job_id
    job_data["chosen_worker"] = chosen_worker

    # Add job to the queue and set initial status
    job_queue.put(job_data)
    job_status[job_id] = "queued"

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "chosen_worker": chosen_worker
    }), 201

# Endpoint to check job status
@app.route('/job_status/<job_id>', methods=['GET'])
def job_status_check(job_id):
    status = job_status.get(job_id, "unknown")
    return jsonify({"job_id": job_id, "status": status})

# Worker function to process jobs in the background
def job_worker():
    while True:
        job_data = job_queue.get()  # Block until a job is available
        job_id = job_data["job_id"]

        # Update job status to "in_progress"
        job_status[job_id] = "in_progress"

        logger.info(f"Processing job {job_id}: {job_data}")

        # Simulate job processing (replace with actual task logic)
        time.sleep(5)  # Simulate job work (can be replaced with actual computation)

        # Mark job as completed after processing
        job_status[job_id] = "completed"
        job_queue.task_done()

# Start the worker thread for background job processing
threading.Thread(target=job_worker, daemon=True).start()

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

# Function to start Dask worker on an external node
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

# Start the Flask app
def start_flask_app():
    app.run(host=SCHEDULER_IP, port=SCHEDULER_PORT)

# Main function to set up scheduler and workers
def main():
    logger.info("Setting up the Dask scheduler and workers...")
    
    # Step 1: Set up the Dask Scheduler
    scheduler = Scheduler()
    scheduler.start()

    # Step 2: Reserve and start external workers
    reserved_nodes = check_and_reserve_resources()
    for node in reserved_nodes:
        node_ip = get_node_ip(node)
        if node_ip:
            start_ssh_worker(node, node_ip)

    # Step 3: Start Flask API for job management
    threading.Thread(target=start_flask_app, daemon=True).start()

    logger.info(f"Dask Scheduler is running at: {SCHEDULER_IP}:{SCHEDULER_PORT}")
    
    # Step 4: Wait for worker job submissions
    try:
        while True:
            time.sleep(10)  # Wait for the Flask app to continue processing jobs
    except KeyboardInterrupt:
        logger.info("Dask scheduler and Flask app stopped.")
        scheduler.close()

if __name__ == "__main__":
    main()
