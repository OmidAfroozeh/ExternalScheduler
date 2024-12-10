
#!/bin/bash
#SBATCH --job-name=mpi_test      # Job name
#SBATCH --output=mpi_output.txt  # Output file
#SBATCH --ntasks=4               # Number of tasks (processes)
#SBATCH --time=00:05:00          # Max execution time

# Load MPI module if necessary
module load mpi

# Run the MPI program
mpirun -np 4 ./mpi_program        # Adjust the number of processes as needed
