import pandas as pd

# Load the dataset
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
df = pd.read_parquet(url)

# Filter for the specific month (to exclude any late/early entries)
df_nov = df[(df['lpep_pickup_datetime'] >= '2025-11-01') & 
            (df['lpep_pickup_datetime'] < '2025-12-01')]

# Filter for trip distance <= 1 mile
short_trips = df_nov[df_nov['trip_distance'] <= 1]

# Get the count
print(f"Number of short trips: {len(short_trips)}")