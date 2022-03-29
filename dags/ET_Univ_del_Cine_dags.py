"""
PT172-67
Configurar un Python Operators, para que extraiga información de la base de datos
utilizando el .sql disponible en el repositorio base de las siguientes universidades:
Universidad Del Cine
Dejar la información en un archivo .csv dentro de la carpeta files.
"""
from datetime import datetime
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

import sqlparse

parent_folder = Path(__file__).resolve().parent.parent

def get_data_cine():
    sql_src = os.path.join(parent_folder, 'include/universidad_del_cine.sql')
    with open(sql_src, 'r') as sqlfile:
        query = sqlfile.read()
    query = sqlparse.format(query, strip_comments=True).strip()
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data=data)
    header = ['universities', 'careers', 'inscription_dates', 'names', 'sexo', 'birth_dates', 'locations', 'direccion', 'emails']
    df.to_csv(os.path.join(parent_folder, 'files/ET_Univ_del_Cine.csv'), header=header, index=False)
    return data

with DAG(
    'python_operator_cine',
    start_date=datetime(2020, 3, 26),
    template_searchpath=os.path.join(parent_folder, 'include'),
    catchup=False,
    tags=['operator']
) as dag:
    t0 = DummyOperator(task_id='start')
    t1 = PythonOperator(
        task_id='create_csv',
        python_callable=get_data_cine,
        dag=dag,
    )
    t2 = DummyOperator(task_id='end')

t0 >> t1 >> t2
