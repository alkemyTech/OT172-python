# Dag with Dummy operator for to designed tasks

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=60)
}

with DAG('query_univ_DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022,3,19)
         ) as dag:
         tarea_1= DummyOperator(task_id= 'UnivLaPampa')
         tarea_2= DummyOperator(task_id= 'Univ_Interamericana')

         tarea_1
         tarea_2
         