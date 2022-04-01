"""
DAG configuration, with no process or consults.
Functions inside DAG will use PythonOperator and PostgresOperator
Data will be processed with Pandas and loaded to S3
"""


import logging

from datetime import datetime, timedelta
from airflow import DAG

logging.basicConfig(
    filename='log',
    encoding='utf-8',
    datefmt='%Y-%m-%d',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.debug
)


with DAG (
    'universidad_del_salvador',
    description = 'DAG para la universidad del Salvador',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,24)
) as dag:
    def task_1():
        pass
    def task_2():
        pass
    def task_3():
        pass

    task_1 >> task_2 >> task_3
