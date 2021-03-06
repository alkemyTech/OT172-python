<<<<<<< HEAD
from asyncio import Task
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime
from airflow.models import Connection
from airflow import settings
from sympy import Id
import pandas as pd
import logging
import os
import sys
from decouple import config
=======
import os
from airflow import DAG
import datetime
from datetime import datetime, timedelta
from decouple import config

import pathlib
import logging
from lib.functios import *
from lib.transform_all_data import *


# Credentials,  path & table id:

TABLE_ID= 'Univ_nacional_tres_de_febrero'
PG_ID= config('PG_ID', default='')

>>>>>>> main
# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
import pathlib

path_p = (pathlib.Path(__file__).parent.absolute()).parent
  
sys.path.append(f'/{path_p}/lib')
from functions_Project_1 import *

# Credentials,  path & table id:

TABLE_ID= 'Univ_tecnologica_nacional'
PG_ID= config('PG_ID', default='')
S3_ID=config('S3_ID', default='')
BUCKET_NAAME=config('V', default='')
PUBLIC_KEY=config('PUBLIC_KEY', default='')

# Functions
# Function to define logs, using the logging library: https:/docs.python.org/3/howto/logging.html

<<<<<<< HEAD
=======


>>>>>>> main
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
# Dag definition for the ETL process
<<<<<<< HEAD
with DAG('ETL_Univ_nacional_tres_de_febrero',
         start_date = datetime(2022, 3, 15),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path_p}/airflow/include',
         catchup=False,
         ) as dag:

=======

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
>>>>>>> main

# PythonOperator for extraaction function, commented above
    ET_task = PythonOperator(
        task_id="Extraction",
        python_callable=extraction_transform_data,
        op_kwargs={'database_id': 'training_db',
                   'table_id': TABLE_ID}

    )
# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger,
<<<<<<< HEAD
        op_args= {f'{path_p}/logs/{TABLE_ID}'}
=======
        op_args= {f'{path}/logs/{TABLE_ID}'}
>>>>>>> main
    )



<<<<<<< HEAD
    logging_task
    ET_task
=======
    logging_task 
    extraction_task
>>>>>>> main
