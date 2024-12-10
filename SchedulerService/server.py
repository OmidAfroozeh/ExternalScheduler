from flask import Flask, request, jsonify
from queue import Queue
import threading
import uuid
import random

app = Flask(__name__)

job_queue = Queue()  # Queue for incoming jobs
job_status = {}  # Track job status
worker_registry = {}  # Dictionary to store registered workers

# Endpoint to submit a job
@app.route('/submit_job', methods=['POST'])
def submit_job():
    job_id = str(uuid.uuid4())
    job_data = request.get_json()
    workers = job_data.get("workers")

    if not workers or not isinstance(workers, list):
        return jsonify({"error": "Invalid or missing 'workers'"}), 400

    # Log or process additional worker information
    for worker in workers:
        print(f"Worker Info: {worker}")

    # Choose a worker based on custom logic (here, randomly for simplicity)
    chosen_worker = random.choice(workers)["address"]
    job_data["job_id"] = job_id
    job_data["chosen_worker"] = chosen_worker

    job_queue.put(job_data)
    job_status[job_id] = "queued"

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "chosen_worker": chosen_worker
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
    app.run(port=5000)
