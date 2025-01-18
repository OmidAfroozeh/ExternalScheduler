#imports
import dask
import os
import sys
import time
import re
import distributed
import pandas as pd
import datetime as dt
from dask.distributed import Client, performance_report
import dask.dataframe as dd
from dask_sql import Context
import PipeLine

#Pipeline imports
import gc
import numpy as np
import pandas as pd
import warnings
from tqdm import tqdm
import dask.dataframe as dd
from dask.distributed import Client


#CONSTANTS
DEBUG = False
DISTRIBUTED = False
TIMEFORMAT = "%a-%d-%b-%Y %H:%M:%S"
DATA_FILE = './data/NYC_taxi_trips/yellow_tripdata_2014-02.parquet' # Base_File

MOVE_TO_SCRATCH_DIR = '/var/scratch/dsys2471'
MOVE_TO_MAIN_DIR = '/home/dsys2471'


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
client = ''
USER = 'dsys2471'
PASSWORD = ''
DASHBOARD_PORT = 5005
NODES_AMOUNT = 4
NODE_THREADS = 16
MAINNODE_IP = "127.0.0.1"  #Set later
JOBWORKER_PORT = 5006
JOBWORKER_URL = "127.0.0.1" #Set later
SCHEDULER_URL = f"http://0.0.0.0:6000" # For the Flask app
CLAIM_TIME = '00:20:00'
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
                "memory_used": worker.nbytes,
                "cpu_cores": worker.nthreads,
                "tasks_running": len(worker.processing),
                "bandwidth": worker.bandwidth,
                "time_delay": worker.time_delay,
                # "total_resources": worker.resources,
                # "used_resources": worker.used_resources
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
#Pipeline Functions
#-------------------------------------------------------------------------------------------------------------------------

def read_in_data(file, workload_nr):
    random.seed(workload_nr)
    sample_sizes = [6000000]
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    os.chdir(MOVE_TO_SCRATCH_DIR)
    data = pd.read_parquet(file)
    os.chdir(MOVE_TO_MAIN_DIR)
    data =  data.sample(random.choice(sample_sizes), replace= True, random_state = workload_nr)
    data = data.reset_index().drop('index', axis = 1)
    return data

def create_relevance(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    train_df['relevance'] = train_df['booking_bool'] * 2 + (train_df['click_bool'] * (1 - train_df['booking_bool']))
    return train_df

def handle_datetime(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    train_df['year'] = train_df['date_time'].dt.year
    train_df['month'] = train_df['date_time'].dt.month
    train_df['day'] = train_df['date_time'].dt.day
    train_df = train_df.drop(columns=['date_time'])
    return train_df

def remove_outliers(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    num_feats_with_outliers = ['price_usd', 'comp1_rate_percent_diff', 'comp2_rate_percent_diff', 'comp3_rate_percent_diff', 'comp4_rate_percent_diff', 'comp5_rate_percent_diff', 'comp6_rate_percent_diff', 'comp7_rate_percent_diff', 'comp8_rate_percent_diff']

    for feature in num_feats_with_outliers:  # Based on EDA only price_usd & compX_rate_percent_diff
        Q1 = train_df[feature].quantile(0.25)
        Q3 = train_df[feature].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 3 * IQR
        upper_bound = Q3 + 3 * IQR
        
        # Replace outliers with NaN
        train_df.loc[~train_df[feature].between(lower_bound, upper_bound), feature] = np.nan
    return train_df

def select_subset(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    # Calculate the count of missing values in each row
    train_df['missing_count'] = train_df.isnull().sum(axis=1)

    # Sort the dataframe by 'missing_count' in ascending order
    train_df = train_df.sort_values(by='missing_count')

    # Select the top x% of the rows with the least missing values
    top_percentage = 0.75
    cut_off = int(len(train_df) * top_percentage)
    train_df = train_df.iloc[:cut_off]
    print(len(train_df))
    return train_df

def compute_mean_positions(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    mean_positions = train_df[train_df['random_bool'] == False].groupby('prop_id')['position'].mean().rename('mean_train_position')
    return mean_positions

def merge_mean_positions(train_df, mean_positions):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    return train_df.join(mean_positions, on='prop_id')

def compute_prior(df, group_field, value_field):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    # Sum and count values per group
    sums = df.groupby(group_field)[value_field].transform('sum')
    count = df.groupby(group_field)[value_field].transform('count')
    
    # Calculate leave-one-out prior
    prior = (sums - df[value_field]) / (count - 1)
    return prior

def combine_priors(df, click_prior, book_prior):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    df['click_prior'] = click_prior
    df['book_prior'] = book_prior
    return df

def handle_single_records(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    train_df.fillna({'click_prior': train_df['click_bool'].mean()}, inplace=True)
    train_df.fillna({'booking_prior': train_df['booking_bool'].mean()}, inplace=True)
    return train_df

def handle_priors_test_df(test_df,train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    # Priors for click and booking bool from the training set
    test_df['click_prior'] = test_df['prop_id'].map(train_df.groupby('prop_id')['click_bool'].mean())
    test_df['booking_prior'] = test_df['prop_id'].map(train_df.groupby('prop_id')['booking_bool'].mean())

    # Handling cases with only one record per group
    test_df.fillna({'click_prior': train_df['click_bool'].mean()}, inplace=True)
    test_df.fillna({'booking_prior': train_df['booking_bool'].mean()}, inplace=True)
    return test_df

def compute_previous_searches_train_df(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    # Number of occurences "minus the current row"
    train_df['previous_searches'] = train_df.groupby('prop_id')['prop_id'].transform('count') - 1
    return train_df

def compute_previous_searches_test_df(train_df,test_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    test_df['previous_searches'] = test_df['prop_id'].map(train_df.value_counts('prop_id') - 1).fillna(0)
    return test_df

def calculate_booking_counts(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    booking_counts = train_df.groupby(['prop_id', 'srch_destination_id'])['booking_bool'].sum().reset_index()
    booking_counts.rename(columns={'booking_bool': 'booking_count'}, inplace=True)  
    return booking_counts

def merge_booking_count(df,booking_counts):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    df = df.merge(booking_counts, on=['prop_id', 'srch_destination_id'], how='left')
    return df

def calculate_mean_distances(train_df):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    # Calculate the maximum difference in distance to the user within each search query
    train_df['max_distance_diff'] = train_df.groupby('srch_id')['orig_destination_distance'].transform(lambda x: x.max() - x.min())

    # Compute the mean of these maximum differences by property and add it back to the dataset
    mean_distance = train_df.groupby('prop_id')['max_distance_diff'].mean().reset_index()
    mean_distance.rename(columns={'max_distance_diff': 'mean_max_distance_diff'}, inplace=True)
    return mean_distance

def merge_mean_distances(df,mean_distances):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    return df.merge(mean_distances, on='prop_id', how='left')

def calculate_stats(df, feature):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    stats = df.groupby('prop_id')[feature].agg(['mean', 'std']).rename(
        columns={'mean': f'{feature}_mean', 'std': f'{feature}_std'})
    return stats

def merge_stats(df,stats):
    print(cli_style(f"{get_time()}||MOVING TO NEXT STAGE|",GREEN_CLI))
    return df.join(stats, on='prop_id')

#------------------------------------------------------------------------------------------------------------------------
#Pipeline function
#------------------------------------------------------------------------------------------------------------------------
def pipeline(workflow_nr):
    #Read in data
    train_df = client.submit(read_in_data,'sampling_train_data.parquet',workflow_nr)
    test_df = client.submit(read_in_data,'sampling_test_data.parquet',workflow_nr)

    # Creating the relevance target
    train_df = client.submit(create_relevance,train_df)

    # Extract useful features from 'date_time'
    train_df = client.submit(handle_datetime,train_df)
    test_df = client.submit(handle_datetime,test_df)

    #Remove Ourliers
    train_df = client.submit(remove_outliers,train_df)

    #Select Subset
    train_df = client.submit(select_subset,train_df)

    #Feature Engineering

    #Mean position
    mean_positions = client.submit(compute_mean_positions,train_df)  # Exclude records where the results order is random
    train_df = client.submit(merge_mean_positions,train_df, mean_positions)
    test_df = client.submit(merge_mean_positions,test_df, mean_positions)

    # Apply function for click and booking bool
    click_prior = client.submit(compute_prior,train_df, 'prop_id', 'click_bool')
    book_prior = client.submit(compute_prior,train_df, 'prop_id', 'booking_bool')

    train_df = client.submit(combine_priors,train_df, click_prior, book_prior)

    # Handling cases with only one record per group
    train_df = client.submit(handle_single_records,train_df)

    #Handle priors for test_df
    test_df = client.submit(handle_priors_test_df,test_df,train_df)

    #Number of prior searches
    train_df = client.submit(compute_previous_searches_train_df,train_df)
    test_df = client.submit(compute_previous_searches_test_df,train_df, test_df)

    # Aggregate number of bookings for each property and destination combination
    booking_counts = client.submit(calculate_booking_counts,train_df)

    # Merge this count back to the train and test datasets
    train_df = client.submit(merge_booking_count,train_df, booking_counts)
    test_df = client.submit(merge_booking_count,test_df, booking_counts)

    #Mean distances
    mean_distance = client.submit(calculate_mean_distances,train_df)

    train_df = client.submit(merge_mean_distances,train_df,mean_distance)
    test_df = client.submit(merge_mean_distances, test_df,mean_distance)

    #Stat Features
    features_to_stat = ['visitor_hist_starrating', 'visitor_hist_adr_usd', 'prop_starrating', 'prop_review_score', 'prop_location_score1', 'prop_location_score2', 'prop_log_historical_price', 'price_usd', 'orig_destination_distance', 'srch_query_affinity_score', 'srch_length_of_stay', 'srch_booking_window', 'srch_adults_count', 'srch_children_count', 'srch_room_count']  # Perhaps change this based on LightGBM.feature_importances_
    stats = []
    for feature in tqdm(features_to_stat):
        stats.append(client.submit(calculate_stats,train_df,feature))

    for stat in stats:
        train_df = client.submit(merge_stats,train_df,stat)
        test_df = client.submit(merge_stats,test_df,stat)
    return test_df
#-------------------------------------------------------------------------------------------------------------------------
#Main code
#-------------------------------------------------------------------------------------------------------------------------
async def main():
    if not is_flask_app_running():
        os.system("python SchedulerService/server.py &")
        logger.info("Flask app started in background.")
    global client
    policies = ["unpatched","random", "least_memory_utilization", "least_cpu_load", "shortest_bandwidth_latency",
                 "highest_resource_availability", "composite_score"]

    workflow_sizes = [5]
    
    report_name_pattern = 'repn:.*'
    report_name_regex = re.compile(report_name_pattern)

    password_pattern = 'passwd:.*'
    password_regex = re.compile(password_pattern)
    data_pattern = 'data:.*'
    data_regex = re.compile(data_pattern)
    
    if 'dbg' in sys.argv: #check if debug flag is given and enable debug prints
        DEBUG = True
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT IN DEBUG MODE",YELLOW_CLI))

    #if 'patch' in sys.argv: #check if debug flag is given and enable debug prints
    #    distributed.scheduler.decide_worker = custom_decide_worker
    #    Scheduler.add_worker_original = Scheduler.add_worker
    #    Scheduler.add_worker = custom_add_worker
    #
    #    Scheduler.remove_worker_original = Scheduler.remove_worker
    #    Scheduler.remove_worker = custom_remove_worker
    #    
    #    print(cli_style(f"{get_time()}||MONKEY PATCHED",GREEN_CLI))
    
    if len(list(filter(password_regex.match, sys.argv))) == 1:
        global PASSWORD
        PASSWORD = list(filter(password_regex.match, sys.argv))[0][len(password_pattern) - 2:]
        print(cli_style(f"{get_time()}||PASSWORD:{PASSWORD}|",GREEN_CLI))

    #if len(list(filter(report_name_regex.match, sys.argv))) == 1:
    #    report_name = list(filter(report_name_regex.match, sys.argv))[0][len(report_name_pattern) - 2:]
    #    print(cli_style(f"{get_time()}||PASSWORD:{report_name}|",GREEN_CLI))
    #else:
    #    report_name = ''

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
    else:
        #Setup Local Cluster
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT LOCALLY",GREEN_CLI))
        client = Client()

    print(cli_style(f"{get_time()}||client type:{type(client)}|",GREEN_CLI))
    #if len(list(filter(data_regex.match, sys.argv))) == 1: #check nr of data files provided, only except when 1 is given
                                                      #Otherwise Close Program
    #    os.chdir(MOVE_TO_SCRATCH_DIR) 
    #    DATA_FILE = list(filter(data_regex.match, sys.argv))[0][len(data_pattern) - 2:]
    #    print(cli_style(f"{get_time()}||READING IN DATA",GREEN_CLI))
    #    try:
    #        df = dd.read_parquet(DATA_FILE,blocksize = '64MB')
    #        if(DEBUG):
    #            print(cli_style(f"{get_time()}||DataframeSize:{len(df)}",YELLOW_CLI))
    #
    #    except (FileNotFoundError,FileExistsError) as e:
    #        print(cli_style(f"{get_time()}||DATA FILE NOT FOUND",RED_CLI))
    #        print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
    #        return
    #    except Exception as e:
    #        print(cli_style(f"{get_time()}||{e}",RED_CLI))
    #        print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
    #        return
    #    os.chdir(MOVE_TO_MAIN_DIR)

    #else:
    #    print(cli_style(f"{get_time()}||INCORRECT NUMBER OF DATA FILES PROVIDED",RED_CLI))
    #    print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
    #    return
    resulting_completion_times = pd.DataFrame() 
    policies = ["unpatched","random", "least_memory_utilization", "least_cpu_load", "shortest_bandwidth_latency", "highest_resource_availability", "composite_score"]
        
    for policy in policies:
        times = []
        print(cli_style(f"{get_time()}||STARTING {policy} POLICY", GREEN_CLI))
        if policy != 'unpatched':
            payload = {
            "policy": policy
            }
            response = requests.post(f"{SCHEDULER_URL}/change_policy", json=payload)
            if response.status_code == 201:
                print("POLICY CHANGED SUCCESFULLY")
            
            #MONKEY PATCHING
            distributed.scheduler.decide_worker = custom_decide_worker
            Scheduler.add_worker_original = Scheduler.add_worker
            Scheduler.add_worker = custom_add_worker
        
            Scheduler.remove_worker_original = Scheduler.remove_worker
            Scheduler.remove_worker = custom_remove_worker
            
            print(cli_style(f"{get_time()}||MONKEY PATCHED",GREEN_CLI))
        
        for workflow_size in workflow_sizes:
            print(cli_style(f"{get_time()}||EXECUTING WORKFLOWSIZE: {workflow_size}",GREEN_CLI))
            results = []
            start_time = dt.datetime.now()
            with performance_report(filename=f"./results/PipeLine_EXPERIMENT/no_choice/{policy}/{workflow_size}.html"):
                for i in range(workflow_size):
                    results.append(pipeline(i))
                client.gather(results)
            end_time = dt.datetime.now()
            times.append((end_time-start_time).total_seconds())
        resulting_completion_times[policy] = times

    resulting_completion_times.to_csv('./Pipeline_EXPERIMENT.csv')

    await client.close()
    await cluster.close()
   
    
   

    
if __name__ == '__main__':        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())