import os
import requests
"""
This script downloads the nyc taxi dataset in parquet format
"""


# Base URL for the NYC TLC dataset
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Output directory for Parquet files
OUTPUT_DIR = "nyc_taxi_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dataset types and date range
data_types = ["yellow", "green", "fhv"]  # Yellow Taxi, Green Taxi, For-Hire Vehicles
start_year, end_year = 2023, 2023

# Function to download a dataset
def download_parquet(data_type, year, month):
    file_name = f"{data_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}{file_name}"
    output_path = os.path.join(OUTPUT_DIR, file_name)

    try:
        print(f"Downloading {url}...")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            # Save the parquet file
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"Saved {file_name} to {OUTPUT_DIR}")
        else:
            print(f"Failed to download {url}: HTTP {response.status_code}")

    except Exception as e:
        print(f"Error downloading {url}: {e}")

# Iterate over years, months, and dataset types
for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        for data_type in data_types:
            download_parquet(data_type, year, month)
