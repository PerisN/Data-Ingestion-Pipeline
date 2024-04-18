
# Import libraries
import argparse
import zipfile
import os
from time import time
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Extract CSV files from zipped archive 
    zip_path = r'data\zipped.zip'
    csv_path = r'data\csv_files'

    # Create a directory to extract the CSV files
    os.makedirs(csv_path, exist_ok=True)

    # Extract the ZIP file
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(csv_path)
    except Exception as e:
        print(f'Error extracting ZIP file : {e}')

    # Convert CSV files to Parquet files
    parquet_path = r'data\parquet_files'
    os.makedirs(parquet_path, exist_ok=True)

    def convert_csv_to_parquet(csv_file, parquet_file):
        df = pd.read_csv(csv_file)
        df.to_parquet(parquet_file)

    # Iterate through extracted files 
    for root, dirs, files in os.walk(csv_path):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                parquet_file_path = os.path.join(parquet_path, os.path.splitext(file)[0] + '.parquet')
                try:
                    convert_csv_to_parquet(csv_file_path, parquet_file_path)
                except Exception as e:
                    print(f'Error converting CSV files to Parquet: {e}')

    t_start = time()

    # Iterate over each Parquet file
    parquet_files = [f for f in os.listdir(parquet_path) if f.endswith('.parquet')]
    for parquet_file in parquet_files:
        print(f"\nProcessing {parquet_file}...")
        try:   
            # Load the Parquet file into a PyArrow Table
            table = pq.read_table(os.path.join(parquet_path, parquet_file))
            table_name = os.path.splitext(parquet_file)[0]

            # Convert the PyArrow Table to a pandas DataFrame
            df = table.to_pandas()
            
            # Insert the DataFrame into the database
            print(f"Ingesting {parquet_file} into database...")
            df.to_sql(name=table_name, con=engine, index=False, if_exists='fail')
            print(f'Table {table_name} loaded succesfully!')
        except ValueError as e:
            print(f'Table {table_name} already exists in the database')

    t_end = time()   
    print(f'Total time taken was {t_end-t_start:10.3f} seconds.')


if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='Ingest data into Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgress')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')

    args = parser.parse_args()

    main(args)
