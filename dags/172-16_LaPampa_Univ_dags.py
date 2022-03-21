# The goal is to configure the retry for the tasks
# DAG of the following universities:
# Universidad Nacional de La Pampa
# Universidad Interamericana

from asyncio import Task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def logger():
    import logging
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                       filename='logoloro.log', encoding='utf-8', level=logging.DEBUG)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None


#The following configuration establishes a maximum of 5 retries, 
# with an interval of 5 seconds between them.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# The database was configured from Airflow, entering the option
# "connections" of the "Admin" tab and the designated ID, will be
# set as a parameter to "PostgreOperator" class.

with DAG('LaPampa_Univ',
         start_date=datetime(2020, 3, 22),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath='/home/juan/airflow/include',
         catchup=False,
         ) as dag:

    logging_task= PythonOperator(
        task_id= "logging",
        python_callable= logger
    )

    opr_LaPampa = PostgresOperator(
        task_id='LaPampa',
        postgres_conn_id='some_conn',
        sql='sql-Univ_nac_LaPampa.sql'
    )

    
    logging_task
    opr_LaPampa
