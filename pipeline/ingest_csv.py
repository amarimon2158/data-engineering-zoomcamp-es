#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import click
import os


def ingest_csv_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> None:
    # Download CSV file
    filename = url.split('/')[-1]
    
    if not os.path.exists(filename):
        print(f"📥 Downloading {filename}...")
        os.system(f'wget {url}')
        print("✅ Download complete\n")
    else:
        print(f"✅ File {filename} already exists\n")
    
    # Read CSV file
    print(f"📂 Reading CSV file: {filename}")
    
    # For small files like zone lookup, read all at once
    df = pd.read_csv(filename)
    
    print(f"📊 Total rows: {len(df):,}\n")
    
    # Display first rows
    print("First rows:")
    print(df.head())
    print()
    
    # Check data types
    print("Data types:")
    print(df.dtypes)
    print()
    
    # Insert data
    print(f"🔨 Inserting data into {target_table}...")
    t_start = time()
    
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )
    
    t_end = time()
    print(f"✅ Inserted {len(df):,} rows in {t_end - t_start:.2f}s")
    print(f'\n🎉 Done ingesting to {target_table}')


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default='5432', help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', required=True, help='Target table name')
@click.option('--url', required=True, help='URL of the CSV file')
def main(user, password, host, port, db, table, url):
    """
    Ingest CSV data into PostgreSQL database.
    
    Example:
        uv run ingest_csv.py --table zones --url https://example.com/taxi_zone_lookup.csv
    """
    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    print(f"📍 Connecting to: postgresql://{user}:***@{host}:{port}/{db}")
    print(f"🔗 Data URL: {url}\n")
    
    # Ingest data
    ingest_csv_data(
        url=url,
        engine=engine,
        target_table=table
    )


if __name__ == '__main__':
    main()