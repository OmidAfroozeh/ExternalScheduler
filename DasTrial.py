import subprocess
import os

def create_job_script():
    """Create a job script for MPI program execution."""
    job_script = """#!/bin/bash
#SBATCH --time=00:15:00
#SBATCH -N 2
#SBATCH --ntasks-per-node=16

. /etc/bashrc
. /etc/profile.d/lmod.sh
module load openmpi/gcc/64

APP=./cpi
ARGS=""
OMPI_OPTS="--mca btl ^usnic"
MPI_RUN=mpirun

$MPI_RUN $OMPI_OPTS $APP $ARGS
"""
    return job_script

def submit_job(job_script):
    """Submit the job script using SLURM's sbatch."""
    # Save job script to a file
    job_file = 'mpi_job.job'
    with open(job_file, 'w') as f:
        f.write(job_script)

    # Submit the job via SLURM
    try:
        submit_command = ["sbatch", job_file]
        result = subprocess.run(submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            print("Job submitted successfully. Checking job status...")
            job_id = result.stdout.decode().split()[3]  # Get job ID from the output
            return job_id
        else:
            print(f"Error submitting job: {result.stderr.decode()}")
            return None
    except Exception as e:
        print(f"Failed to submit job: {str(e)}")
        return None

def check_job_status(job_id):
    """Check the status of the submitted job."""
    try:
        status_command = ["squeue", "--job", job_id]
        result = subprocess.run(status_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            print(f"Job status:\n{result.stdout.decode()}")
        else:
            print(f"Error checking job status: {result.stderr.decode()}")
    except Exception as e:
        print(f"Failed to check job status: {str(e)}")

def main():
    """Main function to create, submit and check job status."""
    print("Creating job script...")
    job_script = create_job_script()

    print("Submitting job...")
    job_id = submit_job(job_script)

    if job_id:
        print(f"Job {job_id} submitted successfully.")
        check_job_status(job_id)
    else:
        print("Job submission failed.")

if __name__ == "__main__":
    main()
