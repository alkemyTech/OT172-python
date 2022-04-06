from datetime import datetime
from pathlib import Path
from airflow import DAG
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher as SM
import logging
from airflow.hooks.S3_hook import S3Hook
import os
import pandas as pd
import numpy as np
from tenacity import retry
# import sqlparse


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d')
# Por si necesitamos que los logs tengan el nombre del archivo donde se
# encuentra
logger = logging.getLogger(__name__)

parent_folder = Path(__file__).resolve().parent.parent


def get_data_uba():
    sql_src = os.path.join(
        parent_folder,
        'include/universidad_de_buenos_aires.sql')
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
        'universidades',
        'carreras',
        'fechas_de_inscripcion',
        'nombres',
        'sexo',
        'fechas_nacimiento',
        'codigos_postales',
        'direcciones',
        'emails']
    df.to_csv(
        os.path.join(
            parent_folder,
            'files/ET_Univ_Buenos_Aires.csv'),
        header=header,
        index=False)
    return data


def normalize_df_Uni_Buenos_Aires(path_df, path_dfmerge, path_download):
    logger.info(f"ruta del csv con los datos: {path_df}")
    logger.info(f"ruta del csv de cod_postales: {path_dfmerge}")
    logger.info(f"ruta de descarga: {path_download}")

    def dateparse(x): return datetime.strptime(x, '%y-%b-%d')
    df = pd.read_csv(path_df, parse_dates=['fechas_de_inscripcion',
                                           'fechas_nacimiento'],
                     date_parser=dateparse)

    if 'direcciones' in df.columns:
        df = df.drop(['direcciones'], axis=1)

    df.columns = ['university', 'career', 'inscription_date', 'full_name',
                  'gender', 'birth_date', 'postal_code', 'email']

    # Lectura y renombre del csv con postal_code y location
    dfmerge = pd.read_csv(path_dfmerge)
    dfmerge.columns = ['postal_code', 'location']
    dfmerge['location'] = dfmerge['location'].apply(lambda x: x.lower())

    # Creando columnas con valores nulos por defecto
    df['first_name'] = np.nan
    df['last_name'] = np.nan
    print("forcito")
    # Recorrer todas las filas y normalizar fila por fila, columna por columna
    for row_i in range(len(df)):
        # University
        uni = df.loc[row_i, 'university']
        df.loc[row_i, 'university'] = uni.lower().replace("-", " ").strip()

        # Career
        carr = df.loc[row_i, 'career']
        df.loc[row_i, 'career'] = carr.lower().replace("-", " ").strip()

        # inscription_date
        ins_date = df.loc[row_i, 'inscription_date']
        df.loc[row_i, 'inscription_date'] = ins_date.strftime('%Y-%m-%d')

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
            (np.nan, 'female')[gen == 'f'], 'male')[gen == 'm']

        # age
        age = df.loc[row_i, 'birth_date']
        if age > datetime.now():
            # de esta forma calculamos con el formato de año correcto
            age = age.replace(year=age.year - 100)
        df.loc[row_i, 'birth_date'] = relativedelta(datetime.now(), age).years

        # email, no pueden tener espacios pero si guiones entre los caracteres
        email = df.loc[row_i, 'email']
        df.loc[row_i, 'email'] = email.lower().strip("-").replace(" ", "")

    # location, realizaremos un left join
    df = df.merge(dfmerge, on='postal_code', how='left')

    # eliminar y renombrar columnas
    df = df.drop(['full_name'], axis=1).rename(columns={'birth_date': 'age'})
    print("finals")
    # importar a una ruta especifica
    df.to_csv(path_download + 'Uni_Buenos_Aires.txt', sep="|")
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
    'ET_Univ_Buenos_Aires_dags',
    start_date=datetime(2020, 3, 26),
    template_searchpath=os.path.join(parent_folder, 'include'),
    catchup=False,
    tags=['operator']
) as dag:
    t1 = PythonOperator(
        task_id='create_csv',
        python_callable=get_data_uba,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id='normalize_csv',
        python_callable=normalize_df_Uni_Buenos_Aires,
        retries=5,
        op_kwargs={
            'path_df': os.path.join(parent_folder,
                                    'files/ET_Univ_Buenos_Aires.csv'),
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
                                     'files/Uni_Buenos_Aires.txt'),
            'key': 'Uni_buenos_aires.txt',
            'bucketname': 'cohorte-marzo-77238c6a'},
        dag=dag)


t1 >> t2 >> t3


