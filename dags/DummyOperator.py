############## BRANCH 172-32
# 
# 
#  TASK 172-32

# Dag with Dummy operator for to designed tasks
# A PythonOperator will be used both for the connection to the database, and for the query and transformation of the data

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

    query_1_Univ_LaPampa = DummyOperator(task_id='UnivLaPampa',
                            sql='sql_Univ_LaPampa.sql',
                            )
    query_2_Univ_Interamericana = DummyOperator(task_id='Univ_Interamericana',
                            sql='sql_Univ_Interamericana.sql',
                            )

    connection_task= PythonOperator(
        )

    logging_task= PythonOperator(
        )

    ET_task = PythonOperator(
        )

    connection_task
    logging_task
    ET_task
