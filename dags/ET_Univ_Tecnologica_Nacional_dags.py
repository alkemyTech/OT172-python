import os
from airflow import DAG
import datetime
from datetime import datetime, timedelta
from decouple import config
import pathlib
import logging
from lib.connection_func import load_s3
from lib.logg import logger
from lib.transform_all_data import *
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.hooks.S3_hook import S3Hook


# Credentials,  path & table id:

TABLE_ID= 'Univ_tecnologica_nacional'
PG_ID= config('PG_ID', default='')

# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path = (pathlib.Path(__file__).parent.absolute()).parent

# Functions
# Function to define logs, using the logging library: https:/docs.python.org/3/howto/logging.html


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
         template_searchpath=f'{path}/include',
         catchup=False,
         ) as dag:

# PythonOperator for extraaction function, commented above
    extraction_task = PythonOperator(
        task_id="Extraction",
        python_callable=extraction_transform_data,
        op_kwargs={'database_id': 'training_db',
                   'table_id': TABLE_ID}
    )

# PythonOperator for ETL function, commented above
    load_task = PythonOperator(
        task_id="Load",
        python_callable=load_s3_function
    )

    connect_to_s3 = PythonOperator(
            task_id="s3_connection",
            python_callable=get_connection,
            op_kwargs={'buket_name': BUCKET_NAME,
            'conntype': S3_ID,
            'secret_key':S3_SECRET_KEY,
            'public_key':S3_PUBLIC_KEY,
            'id':'s3_con'} 
        )


# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger,
        op_args= {f'{path}/logs/{TABLE_ID}'}
    )

    logging_task 
    extraction_task

