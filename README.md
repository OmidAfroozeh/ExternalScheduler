# ExternalScheduler
## Installation 
To setup the project, you should make a Miniconda environment to use newer python versions on DAS-6:

### Set up Miniconda
- wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
- chmod +x Miniconda3-latest-Linux-x86_64.sh
- ./Miniconda3-latest-Linux-x86_64.sh
- conda create -n myenv python=3.10
- conda activate myenv

### Install additional libraries
  - pip install -r requirements.txt

## Execution
### Server
- python SchedulerService/server.py

### SQL_experimentation
#### Required command line arguments:
- dstr = run distributed, technically optional but then we run on the local machine. In case of DAS-6 this is the main node!
- data:<name of  data file>, data file has to be present in the var/scratch/<user> foler. Thus, data:nyc_taxi_2014_data.parquet reads in the data from var/scratch/<user>/nyc_taxi_2014_data.parquet
#### Optional command line arguments
- dbg = run in debug mode
- passwd: provide the password of the user to automatically connect to the nodes of dask to start the workers

### Pipeline_experimentation
#### Required command line arguments:
- dstr = run distributed, technically optional but then we run on the local machine. In case of DAS-6 this is the main node!
#### Optional command line arguments
- dbg = run in debug mode
- passwd: provide the password of the user to automatically connect to the nodes of dask to start the workers

