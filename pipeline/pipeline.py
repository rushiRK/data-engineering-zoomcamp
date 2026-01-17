import sys

import pandas as pd

month = int(sys.argv[1])
df = pd.DataFrame({"day": [1, 2], "num_passangers": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")
df['month'] = month
print(df.head())
df.to_parquet(f"output_{month}.parquet")

print(f"Running pipeline for day={month}")