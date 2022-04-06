from asyncio import Task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import datetime
from datetime import datetime
import logging
from decouple import config
import os
import pathlib
import sys
# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path_p = (pathlib.Path(__file__).parent.absolute()).parent
  
sys.path.append(f'/{path_p}/lib')
from functions_Project_1 import *

# Credentials,  path & table id:

TABLE_ID= 'Univ_nacional_tres_de_febrero'
PG_ID= config('PG_ID', default='')
S3_ID=config('S3_ID', default='')
BUCKET_NAAME=config('V', default='')
PUBLIC_KEY=config('PUBLIC_KEY', default='')

# root

# root
# Function to define logs, using the logging library: https://docs.python.org/3/howto/logging.html

# Retries configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# Dag definition for the ETL process
with DAG('ETL_Univ_nacional_tres_de_febrero',
         start_date=datetime(2020, 3, 24),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path_p}/airflow/include',
         catchup=False,
         ) as dag:


# PythonOperator for ETL function, commented above
    ET_task = PythonOperator(
        task_id="ET",
        python_callable=extraction_transform_data, 
        op_kwargs={'database_id': PG_ID, 'table_id': TABLE_ID}
    )



# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger
    )

    logging_task
    ET_task 
