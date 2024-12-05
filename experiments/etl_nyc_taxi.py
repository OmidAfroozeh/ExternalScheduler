"""
    This experiment performs various etl tasks on the nyc taxi dataset to evaluate the performance of Dask + our external
    scheduler.
"""

# Path to dataset
dataset_path = '../data/nyc_taxi_dataset/*.parquet'


# Load the dataset into a Dask DataFrame
df = dd.read_csv(dataset_path)

# Display the Dask DataFrame structure (not the full data)
print(df.head())