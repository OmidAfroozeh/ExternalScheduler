import requests

"""
    Contains utility functions needed throughout the project
"""


def send_post_request(url, payload, headers=None):
    """
    Sends an HTTP POST request to the specified URL.

    Args:
        url (str): The endpoint URL.
        payload (dict): The data to send in the POST request body.
        headers (dict, optional): Additional headers for the request.

    Returns:
        dict: The response from the server as a dictionary (if JSON), or None on failure.
    """
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses
        return response.json()  # Assuming the server responds with JSON
    except requests.RequestException as e:
        print(f"Error sending POST request to {url}: {e}")
        return None


def send_get_request(url, headers=None, params=None):
    """
    Sends an HTTP GET request to the specified URL.

    Args:
        url (str): The endpoint URL.
        headers (dict, optional): Additional headers for the request.
        params (dict, optional): Query parameters for the GET request.

    Returns:
        dict: The response from the server as a dictionary (if JSON), or None on failure.
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses
        return response.json()  # Assuming the server responds with JSON
    except requests.RequestException as e:
        print(f"Error sending GET request to {url}: {e}")
        return None


def send_heartbeat(server_url, node_id, node_url):
    """
    Sends a heartbeat to the server to register or update the node's status.

    Args:
        server_url (str): The server's base URL.
        node_id (str): The unique identifier of the node.
        node_url (str): The URL of the node.

    Returns:
        dict: The server's response as a dictionary, or None on failure.
    """
    try:
        url = f"{server_url}/register_node"
        payload = {
            "node_id": node_id,
            "node_url": node_url
        }
        return send_post_request(url, payload)
    except Exception as e:
        print(f"Failed to send heartbeat to server {server_url}: {e}")
        return None


def send_job(server_url, job_data):
    """
    Sends a job to the server.

    Args:
        server_url (str): The server's base URL.
        job_data (dict): The job data to send.

    Returns:
        dict: The server's response as a dictionary, or None on failure.
    """
    try:
        url = f"{server_url}/submit_job"
        return send_post_request(url, job_data)
    except Exception as e:
        print(f"Failed to send job to server {server_url}: {e}")
        return None
