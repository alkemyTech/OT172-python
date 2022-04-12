"""
DAG configuration, with no process or consults.
Functions inside DAG will use PythonOperator and PostgresOperator
Data will be processed with Pandas and loaded to S3
Added retries to the database config
"""

import logging

from datetime import datetime, timedelta
from airflow import DAG

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

logging.basicConfig(
    filename='log',
    encoding='utf-8',
    datefmt='%Y-%m-%d',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.debug
)

with DAG (
    'universidad_de_palermo',
    description = 'DAG para la universidad de Palermo',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,24),
    default_args = DEFAULT_ARGS,


) as dag:
    def task_1():
        pass
    def task_2():
        pass
    def task_3():
        pass

    task_1 >> task_2 >> task_3
