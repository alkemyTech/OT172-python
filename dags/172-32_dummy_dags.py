# TAREA 172-32

# Dag with Dummy operator for to designed tasks

from airflow import DAG
from airflow.operators.dummy import DummyOperator #https://airflow.apache.org/docs/apache-airflow/2.0.1/_api/airflow/operators/dummy/index.html
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator #https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/operators/postgres/index.html
from airflow.operators.python import PythonOperator #https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG('query_univ_DAG',
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath='/home/juan/airflow/include',
         catchup=False,
         ) as dag:
    tarea_1 = DummyOperator(task_id='UnivLaPampa')
    tarea_2 = DummyOperator(task_id='Univ_Interamericana')

    tarea_1
    tarea_2
