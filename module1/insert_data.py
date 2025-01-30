import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = int(params.port)
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "/Users/htweeaintphyu/Learning/DE_DataTalks/yellow_tripdata_2015-01.csv"
    # Download CSV if needed (uncomment to use)
    # os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Start the connection
    engine.connect()

    # Read the data in chunks with size 100000
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # Read the first chunk
    df_chunk = next(df_iter)
    
    # Create schema (table) with name "yellow_taxi_data" in PostgreSQL
    # Use the first chunk to infer the schema
    df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Schema for {table_name} created.")
    
    while True:
        try:
            start = time()
            df_chunk = next(df_iter)
            df_chunk.tpep_pickup_datetime = pd.to_datetime(df_chunk.tpep_pickup_datetime)
            df_chunk.tpep_dropoff_datetime = pd.to_datetime(df_chunk.tpep_dropoff_datetime)
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            end = time()
            print(f"Inserting chunk takes {end - start} seconds.")
        except StopIteration:
            print("All data is inserted completely.")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument('--user', help="user name for postgres")
    parser.add_argument('--password', help="password for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database name for postgres")
    parser.add_argument('--table_name', help="table name where the csv data will be stored")
    parser.add_argument('--url', help="url of csv file")

    args = parser.parse_args()

    main(args)