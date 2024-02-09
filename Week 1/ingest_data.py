#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time
import argparse

# Download the data from the Github location hosted by DatatalksClub and Amazon S3-
# Green Taxi Data - "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
# Taxi Zones - "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"


def main(args):
    user=args['user']
    password=args['password']
    host=args['host']
    port=args['port']
    db=args['db']
    table_name=args['table_name']
    url = args["url"]

    if args.get("filename"):
        filename = args["filename"]
    else:
        filename="yellow_tripdata_2021-02.parquet"

    os.system(f"wget {url} -O {filename}")

    df = pd.read_parquet(filename)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(engine.connect())
    # taxi_zone = pd.read_csv('taxi+_zone_lookup.csv')

    # df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    # output_name='df_cleaned.parquet'
    # df.to_parquet(output_name)

    parquet_file = pq.ParquetFile(filename)
    parquet_size = parquet_file.metadata.num_rows

    # default (and max) batch size
    index = 100000
    step = index
    inc = 1

    for i in parquet_file.iter_batches(batch_size=index, use_threads=True):
        t_start = time()
        print(f'Ingesting {index} out of {parquet_size} rows ({index / parquet_size:.0%})')
        i.to_pandas().to_sql(name=table_name, con=engine, if_exists='append')
        index += step if parquet_size - inc*step > step else parquet_size%index
        inc += 1
        t_end = time()
        print(f'\t- it took %.1f seconds' % (t_end - t_start))


if __name__=="__main__":

    # Create the parser
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    # Add an argument
    parser.add_argument('--user', help="Username for postgres")
    parser.add_argument('--password', help="passowrd for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database for postgres")
    parser.add_argument('--table_name', help="name of the table where results will be written")
    parser.add_argument('--url', help="url for the parquet file")
    parser.add_argument('--filename', help="filename for saving the parquet file")


    # Parse the argument
    args = vars(parser.parse_args())
    main(args)