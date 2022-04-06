import os
from airflow import DAG
import datetime
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from decouple import config
import pathlib
import logging
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
S3_ID=config('S3_ID', default='')
BUCKET_NAAME=config('BUCKET_NAME', default='')
PUBLIC_KEY=config('PUBLIC_KEY', default='')

# Retries configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# Dag definition for the E process
with DAG('Extraction_Univ_LaPamPa',
         description='for the execution of an sql query to the trainingn\
          database, to obtain information about Universidad tres de febrero',
         start_date=datetime(2020, 3, 24),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path_p}/include',
         catchup=False,
         ) as dag:

# PythonOperator for extraaction function, commented above
    extraction_task = PythonOperator(
        task_id="Extraction",
        python_callable=extraction,
        op_kwargs={'database_id': 'training_db',
                   'table_id': TABLE_ID}

    )
# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger,
        op_args= {f'{path_p}/logs/{TABLE_ID}'}
    )

    logging_task 
    extraction_task

