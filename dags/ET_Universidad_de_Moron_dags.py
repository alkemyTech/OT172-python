# Configurar el Python Operator para que ejecute las dos funciones que procese
# los datos para las siguientes universidades:
#       Universidad De Morón
#       Universidad Nacional De Río Cuarto

# The dag runs every 1 hour
# Operators to use: PythonOperator, PostgresHook
# Connection made in ariflow interface
# Use pandas to create .csv file
# create files folder for load .csv files
# the extract task outputs a .csv file. To work with him on the next task


from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
from pathlib import Path
import os
from os import path, makedirs
import logging

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# Config logging
logging.basicConfig(filename='log', encoding='utf-8',datefmt='%Y/%m/%d', 
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

# Rutas
ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
ruta_include = path.abspath(path.join(ruta_base, 'include'))
ruta_files = path.abspath(path.join(ruta_base,'files'))
logging.info(f'ruta_include: {ruta_include}')
logging.info(f'Ruta_files: {ruta_files}')

def extract(query_sql, university):
    # connection
    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    # cursor = connection.cursor()
    logging.info("Connection established successfully.")

    # include and files folder paths
    #ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    #ruta_include = path.abspath(path.join(ruta_base, 'include'))
    #ruta_files = path.abspath(path.join(ruta_base,'files'))
    #logging.info(f'ruta_include: {ruta_include}')
    #logging.info(f'Ruta_files: {ruta_files}')

    # if the files folder does not exist it is created
    if not path.isdir(ruta_files):
        makedirs(ruta_files)
    
    file_m = open(f'{ruta_include}/{query_sql}', 'r')
    query_m = file_m.read()
   
    moron_df = pd.read_sql(query_m, connection)
    moron_df.to_csv(f'{ruta_files}/{university}')
    logging.info('ET_Universidad_de_Moron.csv file created')

def process(university):
    # This function is responsible for processing and normalizing
    # the data obtained in the previous task.
    # Read csv file and create data frame
    logging.info('Start de process function')
    path_df = os.path.join(ruta_files, f'{university}')
    data_frame = pd.read_csv(path_df)
    logging.info('Data frame created')
    


def load():
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
with DAG(
    'ET_Universidad_de_Moron',
    description = 'ETL dag for university Moron',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,20),
    default_args=default_args,
    catchup=False
    
) as dag:

    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        op_kwargs={
            'query_sql': 'Uni_Moron.sql',
            'university': 'ET_Universidad_de_Moron.csv'
        }
        )
    # En el futuro seran cambiados
    process_data = PythonOperator(
        task_id = 'process',
        python_callable = process,
        op_kwargs={
            'university': 'ET_Universidad_de_Moron.csv'
        }
        )
    load_data = PythonOperator(
        task_id = 'load',
        python_callable = load
        )


extract >> process_data >> load_data