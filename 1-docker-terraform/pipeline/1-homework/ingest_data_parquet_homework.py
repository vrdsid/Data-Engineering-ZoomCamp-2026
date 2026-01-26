#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):

    # Parquet source (official TLC CDN)
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'

    # DB engine
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # Read parquet file
    print(f'Downloading parquet file: {url}')
    df = pd.read_parquet(url, engine='pyarrow')

    print(f'Total rows: {len(df)}')

    # Write in chunks
    first = True

    for start in tqdm(range(0, len(df), chunksize)):
        chunk = df.iloc[start:start + chunksize]

        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='replace' if first else 'append',
            index=False
        )

        first = False

    print('Ingestion completed successfully')


if __name__ == '__main__':
    run()
