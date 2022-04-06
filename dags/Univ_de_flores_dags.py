 
 
 

###################################################################################
# Dag creado el dia 2022-04-06 con la siguiente configuración:
#
# Ruta de archivos SQL= rutasql
# 
# Configuraciòn:
# Default args: 
#               'owner': ''airflow'',
#               'depends_on_past': False,
#               'email_on_failure': False,
#               'email_on_retry': False,
#               'retries': 5,
#                retry_delay': timedelta(seconds=30)
# 
# start_date=datetime(startdate_toreplace),
#                max_active_runs= runs_toreplace,
#                schedule_interval= scheduler_toreplace,
#                template_searchpath= rutasql,
#                catchup=False,
#           
#########################################################################################


from Dag_functions import *
from asyncio import Task
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow import settings
from sympy import Id
import pandas as pd
import datetime
from datetime import datetime
import logging
import os
import pathlib
from decouple import config


###### Credentials

PG_USERNAME= config('PG_USERNAME', default='')
PG_PASSWORD= config('PG_PASSWORD', default='')
PG_SCHEMA= config('PG_SCHEMA', default='')
PG_HOST= config('PG_HOST', default='')
PG_CONNTYPE=config(' PG_CONNTYPE', default='')
PG_ID= config('PG_ID', default='')
PG_PORT= config('PORT', default='')


S3_BUCKET_NAME=config('BUCKET_NAME', default='')
S3_ID=config('S3_ID', default='')
S3_SECRET_KEY=config('S3_SECRET_KEY', default='')
S3_PUBLIC_KEY=config('S3_PUBLIC_KEY', default='')



# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path = (pathlib.Path(__file__).parent.absolute()).parent

# root
# Function to define logs, using the logging library: https://docs.python.org/3/howto/logging.html


# Retries configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':5,
    'retry_delay': timedelta(seconds=30)
}

# Dag definition for the ETL process

# Dag definition for the ETL process
with DAG('Univ_de_flores',
         start_date=datetime(2020,4,4),
         max_active_runs=8,
         schedule_interval= '@hourly',
         default_args=default_args,
         template_searchpath=f'{path}/airflow/include',
         catchup=False,
         ) as dag:

# PythonOperator for the execution of get_connection, commented above
    connect_to_pgdb = PythonOperator(
            task_id="pg_connection",
            python_callable=get_connection,
            op_kwargs={'username': PG_USERNAME, 'password': PG_PASSWORD,
                       'db': PG_SCHEMA, 'host': PG_HOST,
                       'conntype': PG_CONNTYPE, 'id': PG_ID, 'port': (PG_PORT)}
    )
# PythonOperator for ETL function, commented above
    ET_task = PythonOperator(
        task_id="ET",
        python_callable=ET_function
    )


    connect_to_s3 = PythonOperator(
            task_id="s3_connection",
            python_callable=get_connection,
            op_kwargs={'buket_name': S3_BUCKET_NAME,
            'conntype': S3_ID,
            'secret_key':S3_SECRET_KEY,
            'public_key':S3_PUBLIC_KEY,
            'id':'s3_con'} 
        )

# PythonOperator for ETL function, commented above
    load_task = PythonOperator(
        task_id="Load",
        python_callable=load_s3_function
    )

# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger
    )

logging_task 
connect_to_pgdb >> ET_task >> connect_to_s3 >> load_task

