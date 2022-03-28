# Configurar un Python Operators, para que extraiga información de la base de datos 
#     utilizando el .sql disponible en el repositorio base de las siguientes universidades: 
#     Universidad Nacional De Jujuy y Universidad De Palermo

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
from os import path, makedirs
import logging

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# Config logging
logging.basicConfig(filename='log', encoding='utf-8',datefmt='%Y/%m/%d', 
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


def extract(query_sql, university):
    # connection
    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    # cursor = connection.cursor()
    logging.info("Connection established successfully.")

    # include and files folder paths
    ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    ruta_include = path.abspath(path.join(ruta_base, 'include'))
    ruta_files = path.abspath(path.join(ruta_base,'files'))
    logging.info(f'ruta_include: {ruta_include}')
    logging.info(f'Ruta_files: {ruta_files}')

    # if the files folder does not exist it is created
    if not path.isdir(ruta_files):
        makedirs(ruta_files)
    
    file_jujuy = open(f'{ruta_include}/{query_sql[0]}', 'r')
    file_palermo = open(f'{ruta_include}/{query_sql[1]}', 'r')
    
    query_j = file_jujuy.read()
    query_p = file_palermo.read()
    
    jujuy_df = pd.read_sql(query_j, connection)
    jujuy_df.to_csv(f'{ruta_files}/{university[0]}')
    logging.info('jujuy.csv file created')
    
    palermo_df = pd.read_sql(query_p, connection)
    palermo_df.to_csv(f'{ruta_files}/{university[1]}')
    logging.info('palermo.csv file created')

def process():
    pass
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
    'Etl_universidad_de_jujuy_palermo',
    description = 'ETL dag for 2 universities group C (jujuy y palermo)',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,20),
    default_args=default_args,
    catchup=False
    
) as dag:

    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        op_kwargs={
            'query_sql': ['universidad_nacional_de_jujuy.sql','universidad_de_palermo.sql'],
            'university': ['jujuy.csv','palermo.csv']
        }
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
