## About:

Running a ETL pipeline on Airflow which will extract the tsv files and transform it into parquet file and finally loading it into Mysql Tables
	
## Requirements:
	
pandas==1.5.3
sqlalchemy==1.4.46
pyarrow
pymysql

Installation of Airflow, Mysql and Python3

## How to run?

Start the Airflow web,

 > airflow webserver --port 8080
 
Start the Airflow scheduler,

 > airflow scheduler
 
 
If not present, create the Airflow DAG folder

	> /home/vineet/airflow/dags

and copy the Telemetry_dag.py file in the Airflow DAG folder/DAG_BAG of Airflow and enable the DAG through the toggle button in the Airflow UI 


The DAG would reflect in the Airflow UI but would be broken. 

## Reason?

Missing variables/keyword for the DAG which needs to be added to the Airflow. There are three variables to add to the Airflow, 


To Add the variables, go to the Admin > Variables in the Airflow UI

### First,

Key : db_connection_string
Val : Database Connection String

### Second,

Key : tsv_folder_path
Val : path for source tsv files

### Third,

Key : parquet_folder_path
Val : path for target parquet files

Once the values are added the DAG error should be gone automatically.

Now you are ready to run the DAG for dataload/ETL, make sure the tsv_folder_path exist with the valid files for processing and create the parquet_folder_path as well and let it be empty

Run the DAG by triggering it from the UI. It should take some time to transform the TSV to batch of parquet files(where different transformation can be appplied) followed by loading it to the destination Mysql Tables and deleting the intermediate parquet files
