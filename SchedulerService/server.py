from flask import Flask, request, jsonify
from queue import Queue
import threading
import uuid
import random
# Initialize Flask app and job queue
app = Flask(__name__)
job_queue = Queue()  # Queue to hold incoming jobs
job_status = {}  # Dictionary to track the status of jobs by job_id


# Endpoint to submit a job
@app.route('/submit_job', methods=['POST'])
def submit_job():
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    job_data = request.get_json()  # Get job details from request body

    # Validate and process the worker IDs in the payload
    worker_ids = job_data.get("worker_ids")
    print("Workers IDs Receieved: \n")
    print(worker_ids)
    print("\n\n")
    if not worker_ids or not isinstance(worker_ids, list):
        return jsonify({"error": "Invalid or missing 'worker_ids' in payload"}), 400

    # Randomly choose a worker from the provided list
    chosen_worker = random.choice(worker_ids)
    job_data["job_id"] = job_id
    job_data["chosen_worker"] = chosen_worker

    # Add job to the queue and set initial status
    job_queue.put(job_data)
    job_status[job_id] = "queued"

    # Respond with the job ID, chosen worker, and initial status
    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "chosen_worker": chosen_worker
    }), 201

# Endpoint to check job status
@app.route('/job_status/<job_id>', methods=['GET'])
def job_status_check(job_id):
    # Get the status of the requested job
    status = job_status.get(job_id, "unknown")
    return jsonify({"job_id": job_id, "status": status})


# Worker function to process jobs in the background
def job_worker():
    while True:
        job_data = job_queue.get()  # Block until a job is available in the queue
        job_id = job_data["job_id"]

        # Update job status to "in_progress"
        job_status[job_id] = "in_progress"

        # Simulate job processing (replace this with actual scheduling logic)
        print(f"Processing job {job_id}: {job_data}")

        # Mark job as completed after processing
        job_status[job_id] = "completed"
        job_queue.task_done()


# Start the worker thread
threading.Thread(target=job_worker, daemon=True).start()

# Run the Flask app
if __name__ == '__main__':
    app.run(port=5000)
