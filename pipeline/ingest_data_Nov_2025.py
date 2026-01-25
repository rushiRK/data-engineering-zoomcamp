
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
    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
    
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