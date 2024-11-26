import requests
import time

# Server configuration
BASE_URL = "http://127.0.0.1:5000"

# Function to submit a job
def submit_job(task_description):
    url = f"{BASE_URL}/submit_job"
    response = requests.post(url, json={"task": task_description})
    if response.status_code == 201:
        job_info = response.json()
        print(f"Job submitted: {job_info}")
        return job_info["job_id"]
    else:
        print(f"Failed to submit job: {response.status_code}")
        return None

# Function to check job status
def check_job_status(job_id):
    url = f"{BASE_URL}/job_status/{job_id}"
    response = requests.get(url)
    if response.status_code == 200:
        job_status = response.json()
        print(f"Job status: {job_status}")
        return job_status["status"]
    else:
        print(f"Failed to retrieve job status: {response.status_code}")
        return None

# Test the server
def test_scheduler():
    # Step 1: Submit a job
    job_id = submit_job("example_task")
    if not job_id:
        print("Job submission failed. Exiting test.")
        return

    # Step 2: Check the status of the job periodically
    status = "queued"
    while status not in ["completed", "unknown"]:
        time.sleep(1)  # Wait a bit before checking the status again
        status = check_job_status(job_id)
        if status == "unknown":
            print("Job ID not found.")
            break

    print("Test completed.")

# Run the test
if __name__ == "__main__":
    test_scheduler()
