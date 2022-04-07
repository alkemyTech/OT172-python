from asyncio import tasks
from datetime import datetime
from pathlib import Path
from airflow import DAG
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher as SM
from airflow.hooks.S3_hook import S3Hook
import logging
import os
import pandas as pd
import numpy as np
# import sqlparse

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d')

# Por si necesitamos que los logs tengan el nombre del archivo donde se
# encuentra
logger = logging.getLogger(__name__)


parent_folder = Path(__file__).resolve().parent.parent


def get_data_cine():
    sql_src = os.path.join(parent_folder, 'include/universidad_del_cine.sql')
    with open(sql_src, 'r') as sqlfile:
        query = sqlfile.read()
    # query = sqlparse.format(query, strip_comments=True).strip()
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data=data)
    header = [
        'universities',
        'careers',
        'inscription_dates',
        'names',
        'sexo',
        'birth_dates',
        'locations',
        'direccion',
        'emails']
    df.to_csv(
        os.path.join(
            parent_folder,
            'files/ET_Univ_del_Cine.csv'),
        header=header,
        index=False)
    return data


def normalize_df_Uni_Cines(path_df, path_dfmerge, path_download):
    logger.info(f"ruta del csv con los datos: {path_df}")
    logger.info(f"ruta del csv de cod_postales: {path_dfmerge}")
    logger.info(f"ruta de descarga: {path_download}")

    def dateparse(x): return datetime.strptime(x, '%d-%m-%Y')

    df = pd.read_csv(path_df, dtype={'age': int},
                     parse_dates=['inscription_dates', 'birth_dates'],
                     date_parser=dateparse)

    if 'locations' in df.columns:
        df = df.drop(['locations'], axis=1)

    df.columns = ['university', 'career', 'inscription_date', 'full_name',
                  'gender', 'age', 'location', 'email']

    dfmerge = pd.read_csv(path_dfmerge)
    dfmerge.columns = ['postal_code', 'location']
    dfmerge['location'] = dfmerge['location'].apply(lambda x: x.lower())

    df['first_name'] = np.nan
    df['last_name'] = np.nan

    for row_i in range(len(df)):
        # University
        uni = df.loc[row_i, 'university']
        df.loc[row_i, 'university'] = uni.lower().replace("-", " ").strip()

        # Career
        carr = df.loc[row_i, 'career']
        df.loc[row_i, 'career'] = carr.lower().replace("-", " ").strip()

        # inscription_date
        df.loc[row_i, 'inscription_date'] = str(
            df.loc[row_i, 'inscription_date'])

        # fist name
        # lista de abreviaciones mas usadas
        list_abrev = [
            'dr',
            'dra',
            'ms',
            'mrs',
            'ing',
            'lic',
            'ph.d',
            'mtro',
            'arq']
        # funcion auxiliar, comprueba que si existe alguna abreviacion de
        # profesion

        def match_check(elem, list_c):
            for i in list_c:
                return True if SM(None, elem.lower(), i).ratio() > 0.6 else ""
            return False

        fname = df.loc[row_i, 'full_name'].lower().split("-")
        fname = fname[1:] if match_check(fname[0], list_abrev) else fname
        df.loc[row_i, 'first_name'] = fname[0]

        # last name
        df.loc[row_i, 'last_name'] = fname[-1]

        # gender
        gen = df.loc[row_i, 'gender']
        df.loc[row_i, 'gender'] = (
            (np.nan, 'female')[gen == 'F'], 'male')[gen == 'M']

        # age, ya esta en formato int64
        age = df.loc[row_i, 'age']
        df.loc[row_i, 'age'] = relativedelta(datetime.now(), age).years

        # location
        loc = df.loc[row_i, 'location']
        df.loc[row_i, 'location'] = loc.lower().replace("-", " ").strip()

        # email, no pueden tener espacios pero si guiones entre los caracteres
        email = df.loc[row_i, 'email']
        df.loc[row_i, 'email'] = email.lower().strip("-").replace(" ", "")

    # postal_code, realizaremos un left join
    df = df.merge(
        dfmerge,
        on='location',
        how='left').drop(
        ['full_name'],
        axis=1)

    # importar a una ruta especifica
    df.to_csv(path_download + '/Uni_Del_Cines.txt', sep="|")
    logger.info(f"Dataset normalizado y descargado")


def upload_to_s3(filepath, key, bucketname):
    logger.info(f"ruta del txt: {filepath}")
    logger.info(f"key: {key}")
    logger.info(f"nombre del bucket: {bucketname}")

    try:
        hook = S3Hook('s3_conn')
        hook.load_file(
            filename=filepath,
            key=key,
            bucket_name=bucketname,
            replace=True)
    except NoCredentialsError as e:
        logger.error(f"No se pudo subir el archivo a S3: {e}")
        raise AirflowException(f"No se pudo subir el archivo a S3: {e}")
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise AirflowException(e)
    except FileNotFoundError as e:
        logger.error("No existe el archivo a subir a S3:" + e)
        raise AirflowException(e)
    except Exception as e:
        logger.error(e)
        raise AirflowException(e)
    logger.info(f"Archivo subido a S3")


with DAG(
    'ET_Univ_del_Cine_dags',
    start_date=datetime(2020, 3, 26),
    template_searchpath=os.path.join(parent_folder, 'include'),
    catchup=False,
    tags=['operator']
) as dag:
    t1 = PythonOperator(
        task_id='create_csv',
        python_callable=get_data_cine,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id='normalize_csv',
        python_callable=normalize_df_Uni_Cines,
        retries=5,
        op_kwargs={
            'path_df': os.path.join(parent_folder,
                                    'files/ET_Univ_del_Cine.csv'),
            'path_dfmerge': os.path.join(parent_folder,
                                         'dataset/codigos_postales.csv'),
            'path_download': os.path.join(parent_folder,
                                          'files/')},
        dag=dag)

    t3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filepath': os.path.join(parent_folder,
                                     'files/Uni_Del_Cines.txt'),
            'key': 'Uni_Del_Cines.txt',
            'bucketname': 'cohorte-marzo-77238c6a'},
        dag=dag)


t1 >> t2 >> t3

