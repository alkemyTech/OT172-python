"""
Procesar los datos obtenidos de la base de datos dentro del DAG

Configurar el Python Operator para que ejecute las dos funciones que procese los datos para las siguientes universidades:
Universidad Nacional De Villa María

Documentación
https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#module-airflow.operators.python
"""
import logging
from datetime import datetime
from pprint import pprint
from pathlib import Path
import os

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import numpy as np
import pandas as pd
import pendulum
import sqlparse

folder = Path(__file__).resolve().parent.parent

def sql_to_csv_data_villa_maria():
    """ Obtener datos sql y convertirlos en datframe """
    sql_src = os.path.join(folder, 'include/universidad_nacional_de_villa_maria.sql')
    with open(sql_src, 'r') as sqlfile:
        query = sqlfile.read()
    query = sqlparse.format(query, strip_comments=True).strip()
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data=data)
    cols = ['university', 'career', 'inscription_date', 'first_name', 'gender', 'age', 'location', 'email']
    df.columns = cols
    df.to_csv(os.path.join(folder, 'files/ET_Univ_Nacional_Villa_Maria.csv'), header=cols, index=False)

def str_trans(col):
    """  Transformaciones string: minúsculas y elimina guión bajo """
    return col.lower().replace('_', ' ').replace('\n', ' ').strip()

def format_date(x):
    """ Cambia el formato de la fecha """
    p1 = datetime.strptime(x, '%d-%b-%y')
    return datetime.strftime(p1, '%Y-%m-%d')

def process_dataframe():
    """ Modifica las columnas del dataframe """
    today = datetime.now()
    df = pd.read_csv(os.path.join(folder, 'files/ET_Univ_Nacional_Villa_Maria.csv'))
    df['university'] = df['university'].apply(str_trans)
    df['career'] = df['career'].apply(str_trans)
    df['email'] = df['email'].apply(str_trans)
    df['first_name'] = df['first_name'].apply(str_trans)
    df['location'] = df['location'].apply(str_trans)
    df['inscription_date'] = df['inscription_date'].apply(format_date)
    df['gender'] = df['gender'].apply(lambda x: 'male' if x == 'M' else 'female')
    get_age = lambda x: (today - datetime.strptime(x, '%d-%b-%y')).days // 365
    df['age'] = df['age'].apply(get_age)
    df['last_name'] = np.nan
    df['postal_code'] = np.nan
    return df.head()

log = logging.getLogger(__name__)

with DAG(
    dag_id='pandas_python_operator',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 4, 3, tz="UTC"),
    catchup=False,
    tags=['pandas'],
) as dag:
    t0 = DummyOperator(task_id='start')

    @task(task_id="create_csv")
    def create_csv():
        """ DAG que crea archivo csv """
        sql_to_csv_data_villa_maria()
        log.info('CSV created')

    @task(task_id="process_dataframe")
    def process_dataframe():
        """ DAG que procesa el dataframe """
        dataframe = process_dataframe()
        log.info('CSV created')

    t1 = create_csv()

    t2 = process_dataframe()

    t3 = DummyOperator(task_id='finish')

t0 >> t1 >> t2 >> t3
