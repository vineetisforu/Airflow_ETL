import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import gc

def convert_csv_to_parquet(input_file_path, output_file_path, drop_option):
    # Read CSV file into a Pandas DataFrame
    try:
        input_file = Path(input_file_path).stem
        output_file = Path(output_file_path).stem
        output_parent = Path(output_file_path).parent
        count = 0
        
        print(f"reading Table {input_file}")
        for df in pd.read_csv(input_file_path, sep='\t', low_memory=False, on_bad_lines="warn", chunksize=50000 ):

            table = pa.Table.from_pandas(df)

            print(r"read Table")
            # Write PyArrow Table to Parquet file
            
            iter_output_file_path = output_file_path + str(count)
            pq.write_table(table, iter_output_file_path)
            print("table written")
            count+=1
            gc.collect()
    except Exception as error:
        print(error)



def convert_csv_to_parquet_wrapper(**kwargs):
    
    input_folder_path = kwargs["tsv_folder_path"]
    output_folder_path = kwargs["parquet_folder_path"]

    drop_option = 'column'  # options: 'row' or 'column'

    print("starting conversion process")
    for r,d,f in os.walk(input_folder_path):
        for file in f:
            input = os.path.join(r,file)
            output = os.path.join(output_folder_path,Path(os.path.join(r,file)).stem.replace(".","_") + ".parquet")
            convert_csv_to_parquet(input, output, drop_option)


def load_parquet_to_db(input_file_path, **kwargs):
    # Read CSV file into a Pandas DataFrame
    try:
        print(Path(input_file_path).stem)
        file = Path(input_file_path).stem
        table = pq.read_table(input_file_path)
        df = table.to_pandas()

        df.columns = [c.lower() for c in df.columns]

        print("writing sample data")
        print(df.head(1))

        connection_string = kwargs["db_connection_string"]
        from sqlalchemy import create_engine
        sql_engine = create_engine(connection_string)
        engine = sql_engine.connect()

        df.to_sql(file, con=engine, if_exists='append')

        print("written sample data")
        print(df.head(1))
        os.remove(input_file_path)
        print("removed file")
        gc.collect()
    except Exception as error:
        print(error)

def parquet_to_db_wrapper(**kwargs):
    input_folder_path = kwargs["parquet_folder_path"]
    for r,d,f in os.walk(input_folder_path):
        for file in f:
            input = os.path.join(r,file)
            load_parquet_to_db(input, **kwargs)


with DAG(dag_id="Telemetry_dag",
         start_date=datetime(2024,1,1),
         schedule_interval="@once",
         catchup=False) as dag:
    
    task1 = PythonOperator(
        task_id="convert_data_to_parquet",
        python_callable=convert_csv_to_parquet_wrapper,
        op_kwargs = {'tsv_folder_path': Variable.get('tsv_folder_path'), 'parquet_folder_path': Variable.get('parquet_folder_path')}
    )

    task2 = PythonOperator(
        task_id="load_data_to_db",
        python_callable=parquet_to_db_wrapper,
        op_kwargs = {'db_connection_string': Variable.get('db_connection_string'), 'parquet_folder_path': Variable.get('parquet_folder_path')},
    )

task1.set_downstream(task2)