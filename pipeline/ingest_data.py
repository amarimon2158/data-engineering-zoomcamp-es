#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import click
import os


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> None:
    # Download Data
    filename = url.split('/')[-1]
    
    if not os.path.exists(filename):
        print(f"📥 Downloading {filename}...")
        os.system(f'wget {url}')
        print("✅ Download complete\n")
    else:
        print(f"✅ File {filename} already exists\n")
    
    # Read parquet file
    print(f"📂 Opening parquet file: {filename}")
    parquet_file = pq.ParquetFile(filename)
    total_rows = parquet_file.metadata.num_rows
    print(f"📊 Total rows: {total_rows:,}\n")
    
    # Get first batch to create table schema
    batch_iterator = parquet_file.iter_batches(batch_size=chunksize)
    first_batch = next(batch_iterator)
    first_chunk = first_batch.to_pandas()
    
    # Display first rows
    print("First rows:")
    print(first_chunk.head())
    print()
    
    # Check data types
    print("Data types:")
    print(first_chunk.dtypes)
    print()
    
    # Check data shape
    print(f"First chunk shape: {first_chunk.shape}\n")
    
    # Create table
    print(f"🔨 Creating table {target_table}...")
    first_chunk.head(n=0).to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"✅ Table {target_table} created\n")
    
    # Insert first chunk
    t_start = time()
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists='append',
        index=False
    )
    t_end = time()
    print(f"✅ Inserted first chunk: {len(first_chunk):,} rows in {t_end - t_start:.2f}s\n")
    
    # Insert remaining chunks
    chunk_count = 1
    for batch in batch_iterator:
        t_start = time()
        
        df_chunk = batch.to_pandas()
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )
        
        t_end = time()
        chunk_count += 1
        print(f"✅ Chunk {chunk_count}: {len(df_chunk):,} rows in {t_end - t_start:.2f}s")
    
    print(f'\n🎉 Done ingesting {total_rows:,} rows to {target_table}')


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default='5432', help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default=None, help='Target table name (auto-generated if not provided)')
@click.option('--taxi-type', 
              type=click.Choice(['yellow', 'green', 'fhv', 'fhvhv'], case_sensitive=False),
              default='yellow',
              help='Type of taxi data (yellow, green, fhv, fhvhv)')
@click.option('--year', type=int, required=True, help='Year of the data (e.g., 2025)')
@click.option('--month', type=int, required=True, help='Month of the data (e.g., 11)')
@click.option('--chunksize', default=100000, help='Chunk size for batch processing')
@click.option('--url-prefix', default='https://d37ci6vzurychx.cloudfront.net/trip-data', 
              help='URL prefix for parquet files')
def main(user, password, host, port, db, table, taxi_type, year, month, chunksize, url_prefix):
    """
    Ingest NYC Taxi data from parquet files into PostgreSQL database.
    
    Examples:
        # Yellow taxi
        uv run ingest_data.py --taxi-type yellow --year 2025 --month 11
        
        # Green taxi
        uv run ingest_data.py --taxi-type green --year 2025 --month 9
        
        # Custom table name
        uv run ingest_data.py --taxi-type green --year 2025 --month 9 --table my_green_trips
    """
    # Auto-generate table name if not provided
    if table is None:
        table = f'{taxi_type}_taxi_trips'
    
    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Construct URL based on taxi type
    url = f'{url_prefix}/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    
    print(f"🚕 Taxi Type: {taxi_type.upper()}")
    print(f"📍 Connecting to: postgresql://{user}:***@{host}:{port}/{db}")
    print(f"📊 Target table: {table}")
    print(f"🔗 Data URL: {url}\n")
    
    # Ingest data
    ingest_data(
        url=url,
        engine=engine,
        target_table=table,
        chunksize=chunksize
    )


if __name__ == '__main__':
    main()