import socket
import subprocess
import logging
import time
import re
import paramiko
from getpass import getpass
from dask.distributed import Client

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_ip_address():
    """
    Get the IP address of the main node.
    Tries to return the correct IP address based on system configuration.
    """
    try:
        # Try getting the system's default IP address by querying the 'hostname -I'
        result = subprocess.run(['hostname', '-I'], stdout=subprocess.PIPE)
        ip_address = result.stdout.decode().strip().split()[1]  # Take the first IP if multiple are returned
        
        # If 'hostname -I' doesn't give a valid address, try using the default route interface
        if not ip_address:
            ip_address = socket.gethostbyname(socket.gethostname())
        
        logger.info(f"Main node IP: {ip_address}")
        return ip_address
    except Exception as e:
        logger.error(f"Failed to get IP address: {str(e)}")
        return None  # Return None in case of failure

# Setup environment variables
USER = 'dsys2470'
SCHEDULER_IP = get_ip_address()
SCHEDULER_PORT = 5000
NODES_AMOUNT = 4

# Initialize a list to store reserved nodes for the current user
reserved_nodes = []

def get_reserved_nodes():
    """
    Get the list of node names currently reserved by the specified user from `preserve`.
    """
    result = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if result.returncode != 0:
        logger.error("Failed to retrieve reserved nodes.")
        return [], 0  # Returning the reserved nodes list and its count

    # Decode the result and split into lines
    node_lines = result.stdout.decode().splitlines()

    # Initialize a set to store reserved nodes for the current user (to avoid duplicates)
    reserved_nodes = set()

    for line in node_lines:
        # Check if the line contains the current user
        if USER in line:
            # Split the line into parts
            parts = line.split()

            # The nodes start at the 8th column (index 7), collect them
            if len(parts) > 7:  # Check if we have enough parts in the line
                nodes = parts[8:]  # Nodes start from the 8th column (index 8)

                # Filter out non-node strings (e.g., nhosts count, timeouts, etc.)
                valid_nodes = [node for node in nodes if node.startswith("node")]
                reserved_nodes.update(valid_nodes)  # Add valid nodes to the set

    # Convert set back to a list, if needed
    reserved_nodes = list(reserved_nodes)

    if reserved_nodes:
        logger.info(f"Reserved nodes for {USER}: {reserved_nodes}")
    else:
        logger.info(f"No reserved nodes found for {USER}.")
    
    return reserved_nodes, len(reserved_nodes)


def check_and_reserve_resources():
    """
    Check if a resource is already reserved for the current user.
    If not, reserve the required number of nodes.
    """
    total_reserved_nodes = len(get_reserved_nodes())

    # Calculate how many additional nodes are needed
    nodes_needed = max(0, NODES_AMOUNT - total_reserved_nodes)

    if nodes_needed > 0:
        logger.info(f"Requesting {nodes_needed} additional nodes to meet the required {NODES_AMOUNT} nodes.")
        subprocess.run(['preserve', '-1', '-#', str(nodes_needed), '-t', '00:00:30'])
    else:
        logger.info("Sufficient nodes are already reserved.")


def get_node_ip(node_name):
    """
    Get the correct IP for a given node. This function returns the first IP address
    associated with the node, preferring the internal network if both IPs exist.
    """
    result = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if result.returncode == 0:
        node_lines = result.stdout.decode().splitlines()
        for line in node_lines:
            if node_name in line:  # Matching the node name in the reservation list
                # Extracting the IPs
                parts = line.split()
                # Assume the IPs are in positions 1 and 2 
                ip_addresses = parts[1:]  # Getting all IPs in the line
                if ip_addresses:
                    # Returning the first available IP address (since there are multiple)
                    return ip_addresses[0]  # Returning the first IP for connection          
    else:
        logger.error(f"Failed to retrieve IP for {node_name}")
        
    logger.info(f"Received IP address for {node_name} is {ip_addresses[0]}")         
    return None

def start_ssh_worker(node_name, ip_address):
    """
    Start the Dask worker on the allocated node using SSH.
    The worker will connect to the scheduler running.
    """
    password = getpass(f"Enter password for {node_name}: ")  # Ask user for SSH password

    # Initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Log the attempt to connect to the remote node
        logger.info(f"Attempting to connect to {node_name} at {ip_address}...")
        
        # Connect to the remote node using SSH
        ssh.connect(node_name, username=USER, password=password)
        logger.info(f"Successfully connected to {node_name} at {ip_address}")
        
        # Start the Dask worker on the remote node, connecting to the scheduler
        logger.info(f"Scheduler IP for Dask worker: {SCHEDULER_IP}")
        
        command = f"dask-worker {SCHEDULER_IP}:{SCHEDULER_PORT} --nthreads 1 --memory-limit 2GB"
        logger.info(f"Executing command: {command}")
        
        stdin, stdout, stderr = ssh.exec_command(command)
        
        # Log output and errors (if any)
        stdout_output = stdout.read().decode()
        stderr_output = stderr.read().decode()
        
        if stdout_output:
            logger.info(f"Dask worker output: {stdout_output}")
        if stderr_output:
            logger.error(f"Dask worker error: {stderr_output}")

        # Close SSH connection
        ssh.close()
        logger.info(f"SSH connection to {node_name} closed.")

    except paramiko.AuthenticationException:
        logger.error(f"Authentication failed for {node_name} at {ip_address}. Please check your credentials.")
        return False
    except paramiko.SSHException as e:
        logger.error(f"SSH connection error to {node_name} at {ip_address}: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to connect to {node_name} ({ip_address}): {e}")
        return False

    return True

def main():
    # Step 1: Get the list of reserved nodes
    reserved_nodes = get_reserved_nodes()

    # Calculate how many additional nodes are needed
    nodes_needed = max(0, NODES_AMOUNT - len(reserved_nodes))

    if nodes_needed > 0:
        logger.info(f"Requesting {nodes_needed} additional nodes to meet the required {NODES_AMOUNT} nodes.")
        check_and_reserve_resources()  # This will reserve the required nodes
        logger.info("Resources reserved successfully. Proceeding with worker setup...")
        logger.info("Waiting a bit before continuing...")
        time.sleep(5)  # Wait for 5 seconds to ensure the reservation process is completed
    else:
        logger.info("Sufficient nodes are already reserved. Skipping reservation.")

    # If there are less than 2 nodes reserved, handle the failure gracefully
    if len(reserved_nodes) < 2:
        logger.error("Not enough nodes reserved. Exiting.")
        return

    # Step 2: Retrieve IP addresses dynamically for each node
    node_ips = []
    for node_name in reserved_nodes:
        ip_address = get_node_ip(node_name)
        if ip_address:
            node_ips.append(ip_address)
        else:
            logger.error(f"Could not retrieve IP address for {node_name}. Exiting.")
            return

    # Step 3: Start Dask workers on the nodes
    for node_name, ip_address in zip(reserved_nodes, node_ips):
        if start_ssh_worker(node_name, ip_address):
            logger.info(f"Successfully started Dask worker on {node_name}")
        else:
            logger.error(f"Failed to start Dask worker on {node_name}")

    # Step 4: Set up Dask client to connect to scheduler
    logger.info(f"Trying to connect our node to main node, which should be at {get_ip_address()} at port {SCHEDULER_PORT}")
    client = Client(f'{SCHEDULER_IP}:{SCHEDULER_PORT}')  # Use the main node IP address and connect to the scheduler
    logger.info(f"Dask Dashboard available at: {client.dashboard_link}")

    # Step 5: Perform some computation (or just keep it alive for testing)
    logger.info("Performing a test Dask computation...")
    import dask.array as da
    x = da.random.random((10000, 10000), chunks=(1000, 1000))
    y = x + x.T
    
    while True:
        result = y.sum().compute()
        logger.info(f"Initial sum of the array is: {result}")

if __name__ == "__main__":
    main()
