
# The dag runs every 1 hour
# Operators to use: PythonOperator, PostgresHook
# Connection made in ariflow interface
# Use pandas to create .csv file
# create csv_files folder for load .csv files
# the extract task outputs a .csv file. To work with him on the next task


from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
from pathlib import Path
import os
import logging

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# Config logging
logging.basicConfig(filename='app.log',datefmt='%Y/%m/%d', 
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


ruta = str(Path().absolute())+'/apache-airflow-aceleracion/airflow/dags/OT172-python'
def extract():


    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    file_flores = open(f'{ruta}/include/universidad_de_flores.sql', 'r')
    file_villa_maria = open(f'{ruta}/include/universidad_nacional_de_villa_maria.sql', 'r')
    
    query_f = file_flores.read()
    query_m = file_villa_maria.read()
    
    flores_df = pd.read_sql(query_f, connection)
    flores_df.to_csv(f'{ruta}/csv_files/flores.csv')
    
    maria_df = pd.read_sql(query_m, connection)
    maria_df.to_csv(f'{ruta}/csv_files/villa_maria.csv')

def process():
    pass
def load():
    pass

with DAG(
    'etl_universidad_flores_villa_maria',
    description = 'ETL dag for 2 universities group A',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,20),
    catchup=False
    
) as dag:

    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract
        )
    # En el futuro seran cambiados
    process_data = PythonOperator(
        task_id = 'process',
        python_callable = process
        )
    load_data = PythonOperator(
        task_id = 'load',
        python_callable = load
        )


extract >> process_data >> load_data