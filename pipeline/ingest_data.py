
"""
#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
        ) -> pd.DataFrame:
    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
    
    first_chunk = next(df_iter)
    first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append")
    
    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name= target_table, con= engine, if_exists='append')
        print(f"Inserted chunk: {len(df_chunk)}")
    
    print(f'done ingesting to {target_table}')

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=4, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'

    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    ingest_data(url=url, engine=engine, target_table=target_table, chunksize=chunksize)


if __name__ == '__main__':
    main()

 """
#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

def ingest_parquet_data(
        file_path: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
        ) -> None:
    """Ingest parquet file into PostgreSQL database"""
    
    print(f"Reading parquet file: {file_path}")
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    print(f"Total rows to insert: {len(df)}")
    
    # Create table with first batch (empty to set schema)
    df.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
    print(f"Table {target_table} created")
    
    # Insert data in chunks
    total_rows = len(df)
    for start in tqdm(range(0, total_rows, chunksize), desc="Inserting chunks"):
        end = min(start + chunksize, total_rows)
        chunk = df.iloc[start:end]
        chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False)
        print(f"Inserted chunk: {len(chunk)} rows (total: {end}/{total_rows})")
    
    print(f'Done ingesting to {target_table}')


def ingest_csv_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
        dtype: dict = None,
        parse_dates: list = None
        ) -> None:
    """Ingest CSV file into PostgreSQL database"""
    # Only pass parse_dates if it's provided and not None/empty
    read_csv_kwargs = {
        'iterator': True,
        'chunksize': chunksize
    }
    
    if dtype:
        read_csv_kwargs['dtype'] = dtype
    if parse_dates:
        read_csv_kwargs['parse_dates'] = parse_dates
    
    df_iter = pd.read_csv(url, **read_csv_kwargs)
    
    first_chunk = next(df_iter)
    first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
        index=False)
    
    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        print(f"Inserted chunk: {len(df_chunk)}")
    
    print(f'done ingesting to {target_table}')


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--file-path', required=True, help='Path to parquet or CSV file')
@click.option('--target-table', required=True, help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--file-type', type=click.Choice(['parquet', 'csv']), default='parquet', help='File type')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, file_path, target_table, chunksize, file_type):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    if file_type == 'parquet':
        ingest_parquet_data(file_path=file_path, engine=engine, target_table=target_table, chunksize=chunksize)
    else:
        # Only use dtype and parse_dates for taxi trip data, not for zone lookup
        if 'zone' in target_table.lower():
            # Zone lookup CSV doesn't need special dtype or parse_dates
            ingest_csv_data(url=file_path, engine=engine, target_table=target_table, chunksize=chunksize)
        else:
            # Taxi trip data needs dtype and parse_dates
            dtype = {
                "VendorID": "Int64",
                "passenger_count": "Int64",
                "trip_distance": "float64",
                "RatecodeID": "Int64",
                "store_and_fwd_flag": "string",
                "PULocationID": "Int64",
                "DOLocationID": "Int64",
                "payment_type": "Int64",
                "fare_amount": "float64",
                "extra": "float64",
                "mta_tax": "float64",
                "tip_amount": "float64",
                "tolls_amount": "float64",
                "improvement_surcharge": "float64",
                "total_amount": "float64",
                "congestion_surcharge": "float64"
            }
            parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
            ingest_csv_data(url=file_path, engine=engine, target_table=target_table, chunksize=chunksize, dtype=dtype, parse_dates=parse_dates)

if __name__ == '__main__':
    main()    

'''
Here is the SQL queryies:
Question 3:
select count(*) as trip_count from green_taxi_data where lpep_pickup_datetime >= '2025-11-01'and lpep_pickup_datetime < '2025-12-01' and trip_distance <= 1;

Question 4:
SELECT DATE(lpep_pickup_datetime) as pickup_day,
       MAX(trip_distance) as max_distance
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;

Question 5:
SELECT tz."Zone",
       SUM(gt."total_amount") as total_amount_sum
FROM green_taxi_data gt
JOIN taxi_zones tz ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt."lpep_pickup_datetime") = '2025-11-18'
GROUP BY tz."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;

Question 6:
SELECT tz_dropoff."Zone" as dropoff_zone,
       MAX(gt."tip_amount") as max_tip
FROM green_taxi_data gt
JOIN taxi_zones tz_pickup ON gt."PULocationID" = tz_pickup."LocationID"
JOIN taxi_zones tz_dropoff ON gt."DOLocationID" = tz_dropoff."LocationID"
WHERE tz_pickup."Zone" = 'East Harlem North'
  AND gt."lpep_pickup_datetime" >= '2025-11-01'
  AND gt."lpep_pickup_datetime" < '2025-12-01'
GROUP BY tz_dropoff."Zone"
ORDER BY max_tip DESC
LIMIT 1;
'''