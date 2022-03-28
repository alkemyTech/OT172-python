# Configurar el Python Operator para que ejecute las dos funciones que procese
# los datos para las siguientes universidades:
#       Universidad Nacional De Río Cuarto

# The dag runs every 1 hour
# Operators to use: PythonOperator, PostgresHook
# Connection made in ariflow interface
# Use pandas to create .csv file
# create files folder for load .csv files
# the extract task outputs a .csv file. To work with him on the next task


from airflow import DAG
from datetime import timedelta, datetime, date
import pandas as pd
from pathlib import Path
import os
from os import path, makedirs
import logging

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# Config logging
logging.basicConfig(filename='log', encoding='utf-8', datefmt='%Y/%m/%d',
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

# Rutas
ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
ruta_include = path.abspath(path.join(ruta_base, 'include'))
ruta_files = path.abspath(path.join(ruta_base, 'files'))
if not path.isdir(ruta_files):
    makedirs(ruta_files)

logging.info(f'ruta_include: {ruta_include}')
logging.info(f'Ruta_files: {ruta_files}')


def extract(query_sql, university):
    # connection
    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    # cursor = connection.cursor()
    logging.info("Connection established successfully.")

    file_m = open(f'{ruta_include}/{query_sql}', 'r')
    query_m = file_m.read()

    moron_df = pd.read_sql(query_m, connection)
    moron_df.to_csv(f'{ruta_files}/{university}')
    logging.info('ET_Universidad_de_Moron.csv file created')


def process(university):
    """ Data normalization for University of Moron """
    # This function is responsible for processing and normalizing
    # the data obtained in the previous task.

    logging.info('Start de process function')

    # read csv file and create data frame
    moron_df = pd.read_csv(f'{ruta_files}/{university}')
    # read csv file codigo poasta and create data frame
    codigo_p_df = pd.read_csv(f'{ruta_base}/dataset/codigos_postales.csv')
    logging.info('Data frame created')

    # Format: lowercase, without '-', '_' or ' '
    moron_df['university'] = moron_df['university'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['career'] = moron_df['career'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['first_name'] = moron_df['first_name'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['last_name'] = moron_df['last_name'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['email'] = moron_df['email'].str.strip(
    ).str.lower()

    # Gender to male o Female
    moron_df['gender'] = moron_df['gender'].str.replace(
        "F", "female").str.replace("M", "male")
    logging.info('normalized data')

    # Generate location column and normalize
    codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")

    dic = dict(
        zip(codigo_p_df['codigo_postal'], codigo_p_df['localidad']))

    def location(codigo):
        location = dic[codigo]
        return location
    moron_df['location'] = moron_df['postal_code'].apply(location)
    logging.info('column location created and normalized')
    # inscription_data str %Y-%m-%d format

    def inscription(date_inscription):
        """ Change inscription date format to yyyy-mm-dd"""
        formato = datetime.strptime(date_inscription, '%d/%m/%Y')
        formato = formato.strftime('%Y-%m-%d')
        return formato
    moron_df['inscription_date'] = moron_df['inscription_date'].apply(
        inscription)
    logging.info('Change inscription date format')

    # Edad
    def age(born):
        """ Get the age by your date of birth """
        born = datetime.strptime(born, "%d/%m/%Y").date()
        today = date.today()
        return today.year - born.year - ((today.month,
                                          today.day) < (born.month, born.day))
    moron_df['age'] = moron_df['age'].apply(age)
    logging.info('Get age by the date of birth')

    # moron_df.to_excel(f'{ruta_normalized}/data1.xlsx')

    # Create .TXT file in files folder
    moron_df.to_csv(
        f'{ruta_files}/ET_Universidad_de_Moron.txt', index=None, sep='\t')
    logging.info('File .txt created in files folder')
    logging.info('Finish de process')


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
    description='ETL dag for university Moron',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 20),
    default_args=default_args,
    catchup=False

) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={
            'query_sql': 'Universidad_de_Moron.sql',
            'university': 'ET_Universidad_de_Moron.csv'
        }
    )
    # En el futuro seran cambiados
    process_data = PythonOperator(
        task_id='process',
        python_callable=process,
        op_kwargs={
            'university': 'ET_Universidad_de_Moron.csv'
        }
    )
    load_data = PythonOperator(
        task_id='load',
        python_callable=load
    )


extract >> process_data >> load_data
