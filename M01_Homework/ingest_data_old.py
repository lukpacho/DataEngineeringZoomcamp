# ingest_data.py
import argparse
import subprocess
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from time import time

def convert_to_datetime(df, columns):
    """
    Convert specified columns in the DataFrame to datetime format.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        columns (list): List of column names to convert to datetime.
    
    Returns:
        pd.DataFrame: DataFrame with specified columns converted to datetime.
    """
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column])
    return df


def main(args):
    # Parse the command line arguments
    print("Starting the ingestion process...")
    print(f"Arguments received: {args}")
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    database_name = args.database_name
    table_name = args.table_name
    csv_url = args.csv_url

    # Download the csv file if it is a URL
    if csv_url.startswith("http"):
        csv_file = "data.csv.gz" if csv_url.endswith(".gz") else "data.csv"
        if not Path(csv_file).exists():
            print("Downloading with curl …")
            subprocess.run(
                ["curl", "-L", "--retry", "5", "-o", csv_file, csv_url],
                check=True,
            )

    # Create a SQLAlchemy engine to connect to the PostgreSQL database
    print("Connecting to the database...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    engine.connect()
    print("Connection established.")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    print(f"CSV file {csv_file} loaded successfully.")

    df = next(df_iter)

    print("Processing the first chunk of data...")

    df = convert_to_datetime(df, 
        ['tpep_pickup_datetime', 'tpep_dropoff_datetime',
         'lpep_pickup_datetime', 'lpep_dropoff_datetime']
        )

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df = convert_to_datetime(df, 
                ['tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'lpep_pickup_datetime', 'lpep_dropoff_datetime']
                )
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    print("All batches processed successfully.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        prog='ProgramName',
                        description='Ingest data from a csv file into a SQL database',
                        epilog='Text at the bottom of help')

    # Add arguments to the parser: 
    # user, password, host, port, database_name, table_name, 
    # url of the csv file

    parser.add_argument('--user', help='user name for postgres')           # positional argument
    parser.add_argument('--password', help='password for postgres')       # positional argument
    parser.add_argument('--host', help='host for postgres')               # positional argument
    parser.add_argument('--port', help='port for postgres')               # positional argument
    parser.add_argument('--database_name', help='database name for postgres')  # positional argument
    parser.add_argument('--table_name', help='table name where we will write the results to')        # positional argument
    parser.add_argument('--csv_url', help='url of the csv file')      # positional argument

    args = parser.parse_args()
    # Print the parsed arguments for debugging
    print(f"Parsed arguments: {args}")
    main(args)
    print("Ingestion process completed.")