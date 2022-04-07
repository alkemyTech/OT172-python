# Esta funcion obtiene los datos mediante el archivo .sql
# para las dos universidades, Moron y Rio Cuarto
# Crea un archivo .csv con los datos


import pandas as pd
import os
from os import path, makedirs
import logging


#from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# Config logging
logging.basicConfig(filename='log', encoding='utf-8', datefmt='%Y/%m/%d',
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)
try:
    # Rutas
    ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    ruta_include = path.abspath(path.join(ruta_base, 'include'))
    ruta_files = path.abspath(path.join(ruta_base, 'files'))
    if not path.isdir(ruta_files):
        makedirs(ruta_files)

    logging.info(f'ruta_include: {ruta_include}')
    logging.info(f'Ruta_files: {ruta_files}')
except:
    logging.error('Error getting path')


def extract(query_sql, university):
    """ extract university data and create a csv file """
    try:
        # connection
        pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
        connection = pg_hook.get_conn()
        # cursor = connection.cursor()
        logging.info("Connection established successfully.")

        file_m = open(
            f'{ruta_include}/{query_sql}', 'r')
        query_m = file_m.read()

        rcuarto_df = pd.read_sql(query_m, connection)
        rcuarto_df.to_csv(
            f'{ruta_files}/{university}')
        logging.info(f'{university} file created')
    except:
        logging.error('Error to create csv file')
