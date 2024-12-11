#imports
import sys
import time
import re
import distributed
import pandas as pd
import datetime as dt
from dask.distributed import Client, performance_report
import dask.dataframe as dd
from dask_sql import Context
from typing import Set

#CONSTANTS
DEBUG = False
DISTRIBUTED = False
TIMEFORMAT = "%a-%d-%b-%Y %H:%M:%S"
DATA_FILE = './data/NYC_taxi_trips/yellow_tripdata_2014-02.parquet' # Base_File
TABLE_NAME = "NYC_taxi_trips"


#---------------------------------------------------------------------------------------------------------------------------
#CLI styling: turn a string into a colored string FROM https://stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
#---------------------------------------------------------------------------------------------------------------------------
#Available colors
cli_styling = {'HEADER' : '\033[95m',
    'OKBLUE' : '\033[94m',
    'OKCYAN' : '\033[96m',
    'OKGREEN' : '\033[92m',
    'WARNING' : '\033[93m',
    'FAIL' : '\033[91m',
    'ENDC' : '\033[0m',
    'BOLD' : '\033[1m',
    'UNDERLINE' : '\033[4m'}

#cli colors to be used
YELLOW_CLI = 'WARNING'
GREEN_CLI = 'OKGREEN'
RED_CLI = 'FAIL'

#function to call to style CLI text
def cli_style(text, type): 
    return f"{cli_styling[type]}{text}{cli_styling['ENDC']}"
#-------------------------------------------------------------------------------------------------------------------------


def get_time(): #get a string timestamp in the specified TIMEFORMAT
    return dt.datetime.now().strftime(TIMEFORMAT)

#-------------------------------------------------------------------------------------------------------------------------
#DAVID CODE
#-------------------------------------------------------------------------------------------------------------------------

import asyncio
import subprocess
import logging
import time
import distributed
import paramiko
import uuid
import random
from queue import Queue
import threading
import socket
import psutil
import os
from dask import array as da
from dask.distributed import Scheduler as DistributedScheduler
from dask.distributed import LocalCluster, Client
from distributed.scheduler import Scheduler, WorkerState, TaskState, decide_worker
from typing import Callable, Any
from getpass import getpass
import utils as utils
import time
from dask.distributed import performance_report
import distributed.scheduler
import requests  # For HTTP requests



# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables
USER = os.getlogin()
PASSWORD = ''
DASHBOARD_PORT = 8790
NODES_AMOUNT = 4
NODE_THREADS = 16
MAINNODE_IP = "127.0.0.1"  #Set later
JOBWORKER_PORT = 8788
JOBWORKER_URL = "127.0.0.1" #Set later
SCHEDULER_URL = f"http://0.0.0.0:5000" # For the Flask app
CLAIM_TIME = '00:15:00'
INITIAL_LOCAL_WORKERS = 0 # Initial local workers.
WORKERS_PER_NODE = 'auto' # Number of workers for the external nodes, set to auto to auto determine based on available cpu cores.
NODE_REQUEST_TIMEOUT = 15 # Reattempt time to receive nodes.

# Custom add_worker method
async def custom_add_worker(self, comm, address: str, **kwargs):
    # Register the worker via HTTP
    try:
        response = requests.post(
            f"{SCHEDULER_URL}/register_worker",
            json={"worker_id": address}
        )
        if response.status_code == 201:
            print(f"Worker {address} successfully registered.")
        else:
            print(f"Failed to register worker {address}: {response.json()}")
    except Exception as e:
        print(f"Error registering worker {address}: {e}")

    # Call the original add_worker method
    await Scheduler.add_worker_original(self, comm, address=address, **kwargs)

# Custom remove_worker method
async def custom_remove_worker(
        self,
        address: str,
        *,
        stimulus_id: str,
        expected: bool = False,
        close: bool = True,
):
    # Deregister the worker via HTTP
    try:
        response = requests.post(
            f"{SCHEDULER_URL}/remove_worker",
            json={"worker_id": address}
        )
        if response.status_code == 200:
            print(f"Worker {address} successfully removed.")
        else:
            print(f"Failed to remove worker {address}: {response.json()}")
    except Exception as e:
        print(f"Error removing worker {address}: {e}")

    # Call the original remove_worker method
    return await self.remove_worker_original(
        address=address,
        stimulus_id=stimulus_id,
        expected=expected,
        close=close,
    )

# Monkey Patching
Scheduler.add_worker_original = Scheduler.add_worker
Scheduler.add_worker = custom_add_worker

Scheduler.remove_worker_original = Scheduler.remove_worker
Scheduler.remove_worker = custom_remove_worker

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

MAINNODE_IP = get_ip_address()  # Update main node IP dynamically
JOBWORKER_URL = f"tcp://{MAINNODE_IP}:{JOBWORKER_PORT}"

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
    logger.info(f"Currently {total_reserved_nodes} nodes reserved. Trying to reserve more...")
    nodes_needed = NODES_AMOUNT - total_reserved_nodes
    subprocess.run(['preserve', '-1', '-#', str(nodes_needed), '-t', CLAIM_TIME])
    while total_reserved_nodes < NODES_AMOUNT:
        reserved_nodes, total_reserved_nodes = get_reserved_nodes()
        logger.info(f"Reserved nodes amount: {total_reserved_nodes}, wanted nodes: {NODES_AMOUNT}, Check status valid: {total_reserved_nodes > NODES_AMOUNT}")
        if (total_reserved_nodes > NODES_AMOUNT):
            logger.info(f"Sufficient nodes reserved. We have {len(reserved_nodes)} nodes.")
        else:
            logger.info(f"Not sufficient nodes reserved, trying again in {NODE_REQUEST_TIMEOUT} seconds.")
        time.sleep(NODE_REQUEST_TIMEOUT)
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
        logger.info(f"Attempting to start worker node at {node_name} to scheduler at {JOBWORKER_URL}")
        ssh.connect(node_name, username=USER, password=get_password(node_name))
        
        # Command to start the Dask worker
        command = f"nohup dask-worker {JOBWORKER_URL} --nthreads {NODE_THREADS} --nworkers {WORKERS_PER_NODE} > dask-worker.log 2>&1 &"
        
        # Execute the command
        stdin, stdout, stderr = ssh.exec_command(command)
        
        # Log the output and error
        stdout_output = stdout.read().decode('utf-8')
        stderr_output = stderr.read().decode('utf-8')
        
        if stdout_output:
            logger.info(f"STDOUT from {node_name}:\n{stdout_output}")
        if stderr_output:
            logger.error(f"STDERR from {node_name}:\n{stderr_output}")
        
        ssh.close()
    except Exception as e:
        logger.error(f"Failed to connect or execute command on {node_name}: {e}")
        return False
    return True

# Custom worker selection logic
# Custom decide_worker method (no changes needed from your provided code)
def custom_decide_worker(
        ts: TaskState,
        all_workers: set[WorkerState],
        valid_workers: set[WorkerState] | None,
        objective: Callable[[WorkerState], Any],
) -> WorkerState | None:
    """
    Custom logic to override Dask's original decide_worker function.
    This version prints the available workers and the chosen worker to test the override.
    """
    #print("EXECUTING: custom_decide_worker()")
    assert all(dts.who_has for dts in ts.dependencies)

    # Determine the candidates
    if ts.actor:
        candidates = all_workers.copy()
    else:
        candidates = {wws for dts in ts.dependencies for wws in dts.who_has or ()}
        candidates &= all_workers
    if valid_workers is None:
        if not candidates:
            candidates = all_workers.copy()
    else:
        candidates &= valid_workers
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.loose_restrictions:
                    return decide_worker(ts, all_workers, None, objective)

    if not candidates:
        return None
    elif len(candidates) == 1:
        return next(iter(candidates))
    else:
        # Prepare worker information for the payload
        workers_info = [
            {
                "address": worker.address,
                "memory_limit": worker.memory_limit,
                # "memory_used": worker.memory,
                "cpu_cores": worker.nthreads,
                "tasks_running": len(worker.processing),
            }
            for worker in candidates
        ]

        payload = {
            "task_id": ts.key,
            "workers": workers_info,
        }

        # Communicate with the external scheduler
        try:
            response = requests.post(f"{SCHEDULER_URL}/submit_job", json=payload)
            if response.status_code == 201:
                chosen_worker_id = response.json().get("chosen_worker")
                for worker in candidates:
                    if worker.address == chosen_worker_id:
                        return worker
        except Exception as e:
            print(f"Error communicating with external scheduler: {e}")
        return None
    
def is_flask_app_running():
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if 'python' in proc.info['name'] and 'SchedulerService/server.py' in ' '.join(proc.info['cmdline']):
                    logger.info(f"Flask app already running with PID: {proc.info['pid']}")
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False

#-------------------------------------------------------------------------------------------------------------------------
#Main code
#-------------------------------------------------------------------------------------------------------------------------
async def main():
    pattern = 'data:.*'
    regex = re.compile(pattern)
    if len(list(filter(regex.match, sys.argv))) == 1: #check nr of data files provided, only except when 1 is given
                                                      #Otherwise Close Program
        DATA_FILE = list(filter(regex.match, sys.argv))[0][len(pattern) - 2:]
        print(cli_style(f"{get_time()}||READING IN DATA",GREEN_CLI))
        try:
            df = dd.read_parquet(DATA_FILE)
            df = df.repartition(npartitions=10) 

        except (FileNotFoundError,FileExistsError) as e:
            print(cli_style(f"{get_time()}||DATA FILE NOT FOUND",RED_CLI))
            print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
            return
        except :
            print(cli_style(f"{get_time()}||AN UNKOWN ERROR OCCURRED",RED_CLI))
            print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
            return
        
    else:
        print(cli_style(f"{get_time()}||INCORRECT NUMBER OF DATA FILES PROVIDED",RED_CLI))
        print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
        return
    
    if 'dbg' in sys.argv: #check if debug flag is given and enable debug prints
        DEBUG = True
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT IN DEBUG MODE",YELLOW_CLI))

    if 'dstr' in sys.argv: #check if distributed flag is given to run on the DAS6 cluster
        DISTRIBUTED = True
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT DISTRIBUTED",GREEN_CLI))
        #set up DAS-6 cluster

        cluster = await distributed.LocalCluster(n_workers = INITIAL_LOCAL_WORKERS, host="0.0.0.0", dashboard_address=f"0.0.0.0:{DASHBOARD_PORT}", scheduler_port=JOBWORKER_PORT)
        client = await distributed.Client(cluster)

        reserved_nodes = check_and_reserve_resources()
        for node in reserved_nodes:
            node_ip = get_node_ip(node)
            if node_ip:
                start_ssh_worker(node, node_ip)

        #TODO

        print(cli_style(f"{get_time()}||DISTRIBUTED MODE NOT YET IMPLEMENTED",RED_CLI))
    else:
        #Setup Local Cluster
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT LOCALLY",GREEN_CLI))
        client = Client()

    print(cli_style(f"{get_time()}||DASHBOARD AVAILABLE AT:{client.dashboard_link}",GREEN_CLI))

    #Set up Server
    if not is_flask_app_running():
        os.system("python SchedulerService/server.py &")

    #MonkeyPatching
    #distributed.scheduler.decide_worker = custom_decide_worker
    #----------------------------------------------------------------------------------------------------------
    #Execute Query
    #----------------------------------------------------------------------------------------------------------
    with performance_report(filename="./results/test_experiment.html"):    
        c = Context()
        c.create_table(TABLE_NAME, df, persist=True)
        result = c.sql(f"""SELECT SUM(total_amount)
        FROM {TABLE_NAME}
        WHERE passenger_count == 2
        """)
        print(cli_style(f"{get_time()}||EXECUTING SQL QUERY",GREEN_CLI))
        result = result.compute()
        print(result)
   

    
if __name__ == '__main__':        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())