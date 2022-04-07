"""
PT172-43
Configurar los 5 retries para las tareas del DAG de las siguientes universidades:
Universidad De Buenos Aires
"""

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    'retries_cine',
    start_date=datetime(2020, 3, 22),
    max_active_runs=3,
    schedule_interval='@hourly',
    default_args=default_args,
    template_searchpath='/home/lowenhard/airflow/include',
    catchup=False,
    tags=['retries']
) as dag:
    t0 = DummyOperator(task_id='start')

    sql_query = PostgresOperator(
        task_id='uni_cine',
        postgres_conn_id='airflow-universities',
        sql='universidad_de_buenos_aires.sql'
    )

    t0 >> sql_query
