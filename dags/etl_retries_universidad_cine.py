"""
PT172-43
Configurar los 5 retries para las tareas del DAG de las siguientes universidades:
Universidad Del Cine
"""

from asyncio import Task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=20)
}

with DAG('retries_cine',
         start_date=datetime(2020, 3, 22),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath='/airflow/include',
         catchup=False,
         ) as dag:

    operator = PostgresOperator(
        task_id='uni_cine',
        postgres_conn_id='',
        sql='cine.sql'
    )

    operator
