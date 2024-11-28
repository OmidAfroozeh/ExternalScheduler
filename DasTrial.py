import socket
import subprocess
import logging
import time
import paramiko
from getpass import getpass
from dask.distributed import Client

# Setup and logging
USER = 'dsys2470'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_ip_address():
    """
    Get the IP address of the main node (das6).
    Tries to return the correct IP address based on system configuration.
    """
    try:
        # Try getting the system's default IP address by querying the 'hostname -I'
        result = subprocess.run(['hostname', '-I'], stdout=subprocess.PIPE)
        ip_address = result.stdout.decode().strip().split()[0]  # Take the first IP if multiple are returned
        
        # If 'hostname -I' doesn't give a valid address, try using the default route interface
        if not ip_address:
            ip_address = socket.gethostbyname(socket.gethostname())
        
        logger.info(f"Main node IP: {ip_address}")
        return ip_address
    except Exception as e:
        logger.error(f"Failed to get IP address: {str(e)}")
        return None  # Return None in case of failure

def check_and_reserve_resources():
    """
    Check if a resource is already reserved for the current user.
    If not, use `preserve` to reserve two nodes.
    """
    # Run the 'preserve -llist' command to get the current reservation status
    check_reservation = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if check_reservation.returncode != 0:
        logger.error("Failed to check reservations with preserve.")
        return False

    reservation_output = check_reservation.stdout.decode()

    # Check if the current user has an active reservation
    active_reservation = False
    for line in reservation_output.splitlines():
        if USER in line:
            active_reservation = True
            break

    if active_reservation:
        logger.info("Existing reservation found for the user.")
        return True
    else:
        logger.info("No active reservation found for the user, reserving resources.")
        # Reserve 2 nodes for the user
        subprocess.run(['preserve', '-1', '-#', '2', '-t', '00:00:30'])
        return False

def get_reserved_nodes():
    """
    Get the list of nodes currently reserved by the specified user from `preserve`.
    """
    result = subprocess.run(['preserve', '-llist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if result.returncode != 0:
        logger.error("Failed to retrieve reserved nodes.")
        return []

    # Decode the result and split into lines
    node_lines = result.stdout.decode().splitlines()

    # Initialize a list to store reserved nodes for the current user
    reserved_nodes = []

    for line in node_lines:
        # Check if the line contains the current user
        if USER in line:
            # Split the line into parts
            parts = line.split()
            
            # The number of hosts (nhosts) is at index 6, and the nodes are listed after that.
            # We want everything after the 7th element (index 7 onwards), but we skip the nhosts part
            if len(parts) > 7:
                # Slice off the `nhosts` (the number) and start from the nodes
                nodes = parts[7:]  # Nodes start from the 8th column (index 7)
                
                # Only add node names (filter out any non-node strings)
                reserved_nodes.extend([node for node in nodes if node.startswith("node")])

    if reserved_nodes:
        logger.info(f"Reserved nodes for {USER}: {reserved_nodes}")
    else:
        logger.info(f"No reserved nodes found for {USER}.")
    
    return reserved_nodes

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
                # Assume the IPs are in positions 1 and 2 (as per your example)
                ip_addresses = parts[1:]  # Getting all IPs in the line
                if ip_addresses:
                    # Returning the first available IP address (you can refine this selection if needed)
                    return ip_addresses[0]  # Returning the first IP for connection
    else:
        logger.error(f"Failed to retrieve IP for {node_name}")
    return None

def start_ssh_worker(node_name, ip_address):
    """
    Start the Dask worker on the allocated node using SSH.
    The worker will connect to the scheduler running on das6.
    """
    password = getpass(f"Enter password for {node_name}: ")  # Ask user for SSH password

    # Initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Log the attempt to connect to the remote node
        logger.info(f"Attempting to connect to {node_name} at {ip_address}...")
        
        # Connect to the remote node using SSH
        ssh.connect(node_name, username='dsys2470', password=password)
        logger.info(f"Successfully connected to {node_name} at {ip_address}")
        
        # Start the Dask worker on the remote node, connecting to the scheduler on das6
        scheduler_ip = get_ip_address()
        logger.info(f"Scheduler IP for Dask worker: {scheduler_ip}")
        
        command = f"dask-worker {scheduler_ip}:8786 --nthreads 1 --memory-limit 2GB"
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
    # Step 1: Check and reserve resources if necessary
    reservation_status = check_and_reserve_resources()

    if reservation_status:
        logger.info("Skipping reservation, resources are already reserved.")
    else:
        logger.info("Resources reserved successfully. Proceeding with worker setup...")
        # Wait a bit only if we just reserved resources
        logger.info("Waiting a bit before continuing...")
        time.sleep(5)  # Wait for 5 seconds to ensure the reservation process is completed

    # Step 2: Get the list of reserved nodes
    reserved_nodes = get_reserved_nodes()

    # If there are less than 2 nodes reserved, handle the failure gracefully
    if len(reserved_nodes) < 2:
        logger.error("Not enough nodes reserved. Exiting.")
        return

    # Step 3: Retrieve IP addresses dynamically for each node
    node_ips = []
    for node_name in reserved_nodes:
        ip_address = get_node_ip(node_name)
        if ip_address:
            node_ips.append(ip_address)
        else:
            logger.error(f"Could not retrieve IP address for {node_name}. Exiting.")
            return

    # Step 4: Start Dask workers on the nodes
    for node_name, ip_address in zip(reserved_nodes, node_ips):
        if start_ssh_worker(node_name, ip_address):
            logger.info(f"Successfully started Dask worker on {node_name}")
        else:
            logger.error(f"Failed to start Dask worker on {node_name}")

    # Step 5: Set up Dask client to connect to scheduler
    scheduler_ip = get_ip_address()
    client = Client(f'{get_ip_address()}:8786')  # Use the main node IP address and Connect to the scheduler
    logger.info(f"Dask Dashboard available at: {client.dashboard_link}")

    # Step 6: Perform some computation (or just keep it alive for testing)
    logger.info("Performing a test Dask computation...")
    import dask.array as da
    x = da.random.random((10000, 10000), chunks=(1000, 1000))
    y = x + x.T
    
    while True:
        result = y.sum().compute()
        logger.info(f"Initial sum of the array is: {result}")

if __name__ == "__main__":
    main()
