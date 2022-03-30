# Dag for extract, process and upload data to s3
# Dag use data for Universidad de Moron
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

# Config setting for .env

BUCKET_NAME = config('BUCKET_NAME')
PUBLIC_KEY = config('PUBLIC_KEY')
SECRET_KEY = config('SECRET_KEY')


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

        file_m = open(f'{ruta_include}/{query_sql}', 'r')
        query_m = file_m.read()

        moron_df = pd.read_sql(query_m, connection)
        moron_df.to_csv(f'{ruta_files}/{university}')
        logging.info('ET_Universidad_de_Moron.csv file created')
    except:
        logging.error('Error to create csv file')


def process(university):
    """ Data normalization for University of Moron """
    # This function is responsible for processing and normalizing
    # the data obtained in the previous task.
    try:
        logging.info('Start de process function')
        try:
            # read csv file and create data frame
            moron_df = pd.read_csv(f'{ruta_files}/{university}')
            # read csv file codigo poasta and create data frame
            codigo_p_df = pd.read_csv(
                f'{ruta_base}/dataset/codigos_postales.csv')
            logging.info('Data frame created')
        except:
            logging.error('Error reading file csv')

        """ normalize columns: Format: lowercase, without '-', '_' or ' ' """
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

        """ Generate female or male gender column """
        moron_df['gender'] = moron_df['gender'].str.replace(
            "F", "female").str.replace("M", "male")

        """ Generate location column and normalize """
        codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")

        dic = dict(
            zip(codigo_p_df['codigo_postal'], codigo_p_df['localidad']))

        def location(codigo):
            """ Generate location column """
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

        logging.info('End of the process for university of Moron')
        try:
            # Create .TXT file in files folder
            moron_df.to_csv(
                f'{ruta_files}/ET_Universidad_de_Moron.txt', index=None, sep='\t')
            logging.info('File .txt created in files folder')
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

    process = PythonOperator(
        task_id='process',
        python_callable=process,
        op_kwargs={
            'university': 'ET_Universidad_de_Moron.csv'
        }
    )

    upload = PythonOperator(
        task_id='upload',
        python_callable=upload,
        op_kwargs={
            'filename': f'{ruta_files}/ET_Universidad_de_Moron.txt',
            'key': 'ET_Universidad_de_Moron.txt',
            'bucket_name': BUCKET_NAME
        }
    )


extract >> process >> upload
