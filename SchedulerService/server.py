import argparse
from flask import Flask, request, jsonify
from queue import Queue
import threading
import uuid
import random

# Argument parser for the policy
parser = argparse.ArgumentParser(description="Distributed job scheduler with policy selection")
parser.add_argument('--policy', type=str, default="random", choices=["random", "least_memory_utilization",
                                                                  "least_cpu_load", "shortest_bandwidth_latency",
                                                                  "highest_resource_availability", "composite_score"],
                    help="Select the worker selection policy.")
args = parser.parse_args()

# Global variable for policy
policy = args.policy

app = Flask(__name__)

job_queue = Queue()  # Queue for incoming jobs
job_status = {}  # Track job status
worker_registry = {}  # Dictionary to store registered workers

# Function to choose a worker based on a specific policy
def choose_worker(workers, policy):
    if policy == "least_memory_utilization":
        # Choose the worker with the lowest memory utilization percentage
        workers = sorted(workers, key=lambda w: w["memory_used"] / w["memory_limit"])
    elif policy == "least_cpu_load":
        # Choose the worker with the least number of tasks running per CPU core
        workers = sorted(workers, key=lambda w: w["tasks_running"] / w["cpu_cores"])
    elif policy == "shortest_bandwidth_latency":
        # Choose the worker with the shortest bandwidth latency
        workers = sorted(workers, key=lambda w: w["time_delay"])
    elif policy == "highest_resource_availability":
        # Choose the worker with the most unused resources
        workers = sorted(workers, key=lambda w: w["total_resources"] - w["used_resources"], reverse=True)
    elif policy == "composite_score":
        # Custom composite scoring based on memory, CPU, and bandwidth
        def composite_score(w):
            memory_score = 1 - (w["memory_used"] / w["memory_limit"])
            cpu_score = 1 - (w["tasks_running"] / w["cpu_cores"])
            bandwidth_score = 1 / (1 + w["time_delay"])  # Lower delay is better
            return 0.4 * memory_score + 0.4 * cpu_score + 0.2 * bandwidth_score

        workers = sorted(workers, key=composite_score, reverse=True)
    else:
        # Default to random selection if no valid policy is specified
        return random.choice(workers)

    return workers[0] if workers else None

# Job submission endpoint
@app.route('/submit_job', methods=['POST'])
def submit_job():
    job_id = str(uuid.uuid4())
    job_data = request.get_json()
    workers = job_data.get("workers")
    policy_to_use = job_data.get("policy", policy)  # Use global policy if not provided in the job data

    if not workers or not isinstance(workers, list):
        return jsonify({"error": "Invalid or missing 'workers'"}), 400

    # Log or process additional worker information
    for worker in workers:
        print(f"Worker Info: {worker}")

    # Choose a worker based on the specified policy
    chosen_worker = None
    if policy_to_use != "random":
        chosen_worker = choose_worker(workers, policy_to_use)

    if not chosen_worker:
        chosen_worker = random.choice(workers)

    job_data["job_id"] = job_id
    job_data["chosen_worker"] = chosen_worker["address"]

    job_queue.put(job_data)
    job_status[job_id] = "queued"

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "chosen_worker": chosen_worker["address"]
    }), 201


# Endpoint to register a worker
@app.route('/register_worker', methods=['POST'])
def register_worker():
    data = request.get_json()
    worker_id = data.get("worker_id")
    if not worker_id:
        return jsonify({"error": "Missing worker_id"}), 400

    worker_registry[worker_id] = "registered"
    print(f"Registered worker: {worker_id}")
    return jsonify({"status": "registered", "worker_id": worker_id}), 201

# Endpoint to remove a worker
@app.route('/remove_worker', methods=['POST'])
def remove_worker():
    data = request.get_json()
    worker_id = data.get("worker_id")
    if not worker_id:
        return jsonify({"error": "Missing worker_id"}), 400

    if worker_id in worker_registry:
        del worker_registry[worker_id]
        print(f"Removed worker: {worker_id}")
        return jsonify({"status": "removed", "worker_id": worker_id}), 200
    else:
        return jsonify({"error": "Worker not found"}), 404

# Endpoint to check job status
@app.route('/job_status/<job_id>', methods=['GET'])
def job_status_check(job_id):
    status = job_status.get(job_id, "unknown")
    return jsonify({"job_id": job_id, "status": status})

def job_worker():
    while True:
        job_data = job_queue.get()
        job_id = job_data["job_id"]
        job_status[job_id] = "in_progress"
        print(f"Processing job {job_id}: {job_data}")
        job_status[job_id] = "completed"
        job_queue.task_done()

threading.Thread(target=job_worker, daemon=True).start()

if __name__ == '__main__':
    print(f"Using global policy: {policy}")
    app.run(port=5000)
