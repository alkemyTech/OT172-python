# Dag for extract, process and upload data to s3
# Dag use data for Universidad de Rio Cuarto
# The dag runs every 1 hour
# Operators to use: PythonOperator, PostgresHook and S3Hook
# Connection made in ariflow interface
# Use pandas to create .csv file
# create files folder for load .csv files
# the extract task outputs a .csv file. To work with him on the next task
# The process task retur a .txt file
# The upload task save de txt file in S3


from airflow import DAG
from datetime import timedelta, datetime, date
import pandas as pd

import os
from os import path, makedirs
import logging
from decouple import config

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


# Config setting .env

BUCKET_NAME = config('BUCKET_NAME')
PUBLIC_KEY = config('PUBLIC_KEY')

# Config logging
logging.basicConfig(filename='log', encoding='utf-8', datefmt='%Y/%m/%d',
                    format=' %(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

try:
    ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    ruta_normalized = path.abspath(
        path.join(ruta_base, 'lib'))
    ruta_files = path.abspath(path.join(ruta_base, 'files'))
    ruta_include = path.abspath(path.join(ruta_base, 'include'))
    if not path.isdir(ruta_files):
        makedirs(ruta_files)
    logging.info(f'ruta_include: {ruta_include}')
    logging.info(f'Ruta_files: {ruta_files}')
except:
    logging.error('Error getting path')


def extract(query_sql, university):
    """ extract university data and create a csv file """
    # connection
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
        connection = pg_hook.get_conn()
        # cursor = connection.cursor()
        logging.info("Connection established successfully.")

        file_m = open(f'{ruta_include}/{query_sql}', 'r')
        query_m = file_m.read()

        rcuarto_df = pd.read_sql(query_m, connection)
        rcuarto_df.to_csv(f'{ruta_files}/{university}')
        logging.info('ET_Universidad_Nacional_de_Rio_Cuarto.csv file created')
    except:
        logging.error('Error to create csv file')



def process(university):
    """ Data normalization for University of Rio Cuarto"""
    # This function is responsible for processing and normalizing
    # the data obtained in the previous task.

    logging.info('Start de process function')
    try:
        # read csv file and create data frame
        try:
            rcuarto_df = pd.read_csv(
                f'{ruta_files}/{university}')

            # read csv file codigo poasta and create data frame
            codigo_p_df = pd.read_csv(
                f'{ruta_base}/dataset/codigos_postales.csv')
            logging.info('Data frame created')
        except:
            logging.error('Error reading file csv')

        """ normalize columns: Format: lowercase, without '-', '_' or ' ' """
        rcuarto_df['university'] = rcuarto_df['university'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['career'] = rcuarto_df['career'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['first_name'] = rcuarto_df['first_name'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['last_name'] = rcuarto_df['last_name'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['location'] = rcuarto_df['location'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['email'] = rcuarto_df['email'].str.strip(
        ).str.lower()

        """ Generate female or male gender column """
        rcuarto_df['gender'] = rcuarto_df['gender'].str.replace(
            "F", "female").str.replace("M", "male")

        """ Generate postal_code column and normalize """
        codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        # Create a diccionary with codigo_postales.csv
        dic = dict(
            zip(codigo_p_df['localidad'], codigo_p_df['codigo_postal']))

        def codigo(local):
            """ Generate postal_code column """
            codigo = dic[local]
            return codigo
        rcuarto_df['postal_code'] = rcuarto_df['location'].apply(codigo)
        logging.info('column postal_code created and normalized')

        # inscription_data str %Y-%m-%d format
        def inscription(date_inscription):
            """ Change inscription date format to yyyy-mm-dd"""
            formato = datetime.strptime(date_inscription, '%y/%b/%d')
            formato = formato.strftime('%Y-%m-%d')
            return formato
        rcuarto_df['inscription_date'] = rcuarto_df['inscription_date'].apply(
            inscription)
        logging.info('Change inscription date format')

        # Edad
        def age(born):
            """ Get the age by your date of birth """
            born = datetime.strptime(born, "%y/%b/%d").date()
            today = date.today()
            if born.year > today.year:
                born_l = list(str(born))
                born_l[0] = '1'
                born_l[1] = '9'
                born_s = "".join(born_l)
                born = datetime.strptime(born_s, "%Y-%m-%d").date()

            return today.year - born.year - ((today.month,
                                              today.day) < (born.month, born.day))

        rcuarto_df['age'] = rcuarto_df['age'].apply(age)
        logging.info('Get age by the date of birth')

        logging.info('End of the process for university of Rio Cuarto')
        # rcuarto_df.to_excel(f'{ruta_normalized}/data1.xlsx')
        try:
            # Create .TXT file in files folder
            rcuarto_df.to_csv(
                f'{ruta_files}/ET_Universidad_Nacional_de_Rio_Cuarto.txt', index=None, sep='\t')
            logging.info('Create .txt file in files folder')
            logging.info('Finish')
        except:
            logging.error('Error to creating .txt file')
    except:
        logging.error('General error at data normalization')


def upload(filename, key, bucket_name):
    """ upload file to s3 """
    try:
        hook = S3Hook('s3_conn')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name,replace=True)     
        logging.info('The file was saved')
    except:
        logging.error('Error to load file or already exists')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
with DAG(
    'ET_Universidad_Nacional_de_Rio_Cuarto_dags',
    description='ETL dag for university Rio Cuarto',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 20),
    default_args=default_args,
    catchup=False

) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={
            'query_sql': 'Universidad_Nacional_de_Rio_Cuarto.sql',
            'university': 'ET_Universidad_Nacional_de_Rio_Cuarto.csv'
        }
    )
    # En el futuro seran cambiados
    process = PythonOperator(
        task_id='process',
        python_callable=process,
        op_kwargs={
            'university': 'ET_Universidad_Nacional_de_Rio_Cuarto.csv'
        }
    )
    upload = PythonOperator(
        task_id='upload',
        python_callable=upload,
        op_kwargs={
            'filename': f'{ruta_files}/ET_Universidad_Nacional_de_Rio_Cuarto.txt',
            'key': 'ET_Universidad_Nacional_de_Rio_Cuarto.txt',
            'bucket_name': BUCKET_NAME
        }
    )


extract >> process >> upload
