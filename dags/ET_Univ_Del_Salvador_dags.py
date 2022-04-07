"""
DAG configuration, with no process or consults.
Functions inside DAG will use PythonOperator and PostgresOperator
Data will be processed with Pandas and loaded to S3
"""
# MODULES
import logging
from datetime import datetime, timedelta
from airflow import DAG
from pathlib import Path
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np

logging.basicConfig(
    filename='log',
    encoding='utf-8',
    datefmt='%Y-%m-%d',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.debug
)

# creating cvs files
ruta = str(Path().absolute())+'/airflow/OT172-python'
def query_csv():

    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    salvador_sql = open(f'{ruta}/include/univ_del_salvador.sql', 'r')

    salvador_query = salvador_sql.read()
    
    salvador_df = pd.read_sql(salvador_sql, connection)
    salvador_df.to_csv(f'{ruta}/files/salvador.csv')

# INSTANTIATE THE DAG
with DAG (
    'universidad_del_salvador',
    description = 'DAG para la universidad del Salvador',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,24)
) as dag:

    extract = PythonOperator(
                    task_id='Query_Salvador',
                    python_callable=query_csv,
                    op_kwargs={
                        'sql_file': 'query_salvador.sql',
                        'file_name': 'salvador.csv'
                        }
                    )
    process = DummyOperator(task_id="Process_Data")
    load = DummyOperator(task_id="Load_Data")

# DEPENDENCIES
extract >> process >> load