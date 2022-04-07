###################################################################################
# Dag creado el dia fechadia con la siguiente configuración:
#
# Ruta de archivos SQL= rutasql
# 
# Configuraciòn:
# Default args: 
#               'owner': 'owner_toreplace',
#               'depends_on_past': depends_on_past_toreplace,
#               'email_on_failure': email_on_failure_toreplace,
#               'email_on_retry': email_on_retry_toreplace,
#               'retries': retries_toreplace,
#                retry_delay': retry_delay_toreplace
# 
# start_date=datetime(startdate_toreplace),
#                max_active_runs= runs_toreplace,
#                schedule_interval= scheduler_toreplace,
#                template_searchpath= rutasql,
#                catchup=False,
#           
#########################################################################################

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
from pydata_google_auth import default
from sympy import Id
import pandas as pd
import logging
import os
import sys
from decouple import config
# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
import pathlib

path_p = (pathlib.Path(__file__).parent.absolute()).parent
  
sys.path.append(f'/{path_p}/lib')
from functions_Project_1 import *


# Credentials,  path & table id:
PG_ID= config('PG_ID', default='')
S3_ID=config('S3_ID', default='')
TABLE_ID= dagid_toreplace
BUKET_NAME= config('S3_BUCKET_NAME', default='')

# Function to define logs, using the logging library: https://docs.python.org/3/howto/logging.html


# Retries configuration
default_args = {
    'owner': owner_toreplace,
    'depends_on_past': depends_on_past_toreplace,
    'email_on_failure': email_on_failure_toreplace,
    'email_on_retry': email_on_retry_toreplace,
    'retries':retries_toreplace,
    'retry_delay': retry_delay_toreplace
}


# Dag definition for the ETL process
with DAG(dagid_toreplace,
         start_date=datetime(start_date_toreplace),
         max_active_runs=max_active_runs_toreplace,
         schedule_interval= schedule_interval_toreplace,
         default_args=default_args,
         template_searchpath=template_searchpath_toreplace,
         catchup=catchup_toreplace,
         ) as dag:

#
# PythonOperator for extraaction function, commented above
    ET_task = PythonOperator(
        task_id="ET",
        python_callable=extraction_transform_data,
        op_kwargs={'database_id': 'training_db',
                   'table_id': TABLE_ID}

    )
# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger,
        op_args= {f'{path_p}/logs/{TABLE_ID}'}
    )

  # PythonOperator for ETL function, commented above
    load_task = PythonOperator(
        task_id="Load",
        python_callable=load_s3,
        op_kwargs={'id_conn': S3_ID, 'univ': TABLE_Id, 'bucket_name':BUCKET_NAME}
    )

    logging_task >>  ET_task >>   load_task
