from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging
"""
DAG configuration, without queries or processing for "Universidad De Morón"
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
def log_function():
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',filename='logs.log', encoding='utf-8', level=logging.DEBUG)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None

with DAG(
    'elt_university1_groupF',
    description = 'etl for group of universities F (Universidad Nacional De Río Cuarto)',
    schedule_interval = timedelta(hours=1),
    default_args=default_args,
    start_date = datetime(2022, 3, 15)
) as dag:
    logging_task = PythonOperator(
        task_id= "logging",
        python_callable= log_function
    )
    query_task1 = PostgresOperator(
        task_id = "Query_Uni1",
        dag = dag
    )
    transformation_task = PythonOperator(
        task_id = "Transformation",
        dag = dag
    )
    load_task = DummyOperator(
        task_id = "Load",
        dag = dag
    )
    logging_task >> query_task1 >> transformation_task >> load_task