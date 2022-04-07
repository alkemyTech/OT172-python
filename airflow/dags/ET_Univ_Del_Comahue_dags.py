"""
DAG configuration, with no process or consults.
Functions inside DAG will use PythonOperator and PostgresOperator
Data will be processed with Pandas and loaded to S3
"""
# 1 import modules
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

# 2 define default arguments
default_args = {
    'retries': 5,  
    'retry_delay': timedelta(minutes=5)
}

# 3 instantiate the DAG
with DAG (
    'universidad_nacional_del_comahue',
    description = 'DAG para la universidad del comahue',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,24)
) as dag:
# 4 define tasks
    def task_1():
        pass
    def task_2():
        pass
    def task_3():
        pass

# 5 define dependencies
    task_1 >> task_2 >> task_3
