from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import logging

def log_function():
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='logs.log', encoding='utf-8', level=logging.DEBUG)
    return None
"""
DAG configuration, without queries or processing for "Universidad Nacional De Río Cuarto"
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
with DAG(
    'elt_university_Nacional',
    description = 'etl for group of universities F (Universidad Nacional De Río Cuarto)',
    schedule_interval = timedelta(hours=1),
    default_args=default_args,
    start_date = datetime(2022, 3, 15)
) as dag:
    log_conf = PythonOperator(
        task_id = "Logs_configuration",
        python_callable = log_function
    )
    query_task1 = DummyOperator(
        task_id = "Query_Uni1",
        dag = dag
    )
    transformation_task = DummyOperator(
        task_id = "Transformation",
        dag = dag
    )
    load_task = DummyOperator(
        task_id = "Load",
        dag = dag
    )
    log_conf >> query_task1 >> transformation_task >> load_task