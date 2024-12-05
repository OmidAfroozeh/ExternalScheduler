import dask.array as da
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import time  # For keeping the script alive

# Set up the SLURMCluster, specifying the 'defq' partition and other job parameters
cluster = SLURMCluster(
    queue='defq',  # Use the 'defq' partition
    cores=1,  # Number of cores per worker (use the available cores on node)
    memory="1GB",  # Increased memory per worker (adjust as needed)
    processes=1,  # Number of processes per worker (typically 1)
    walltime='00:00:00',  # Adjusted max wall time for job
    job_extra=['--time=00:00:00'],  # Optional: additional job parameters
)

# Scale the cluster to the desired number of workers (e.g., 1 worker, as an example)
cluster.scale(1)

# Connect the Dask client to the cluster
client = Client(cluster)

# Print the Dask dashboard URL to monitor the job
print(f"Dask Dashboard available at: {client.dashboard_link}")

# Simple Dask computation: Create a large random array and perform a computation
x = da.random.random((10000, 10000), chunks=(1000, 1000))  # Create a Dask array
y = x + x.T  # A simple operation: adding the transpose

# Compute the result (this will execute the task graph on the distributed workers)
result = y.sum().compute()

print(f"The sum of the array is: {result}")

# Keep the client alive to monitor the dashboard
print("Waiting for workers to finish...")

# Infinite loop to keep the script running (you can manually stop it when done)
while True:
    result = y.sum().compute()
    print(f"The sum of the array is: {result}")
    time.sleep(20)  # Keeps the program running indefinitely
