"""
PT172-67
Configurar un Python Operators, para que extraiga información de la base de datos
utilizando el .sql disponible en el repositorio base de las siguientes universidades:
Universidad De Villa Maria
Dejar la información en un archivo .csv dentro de la carpeta files.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

import sqlparse

def get_data_villa_maria():
    sql_src = '/home/lowenhard/airflow/include/universidad_nacional_de_villa_maria.sql'
    with open(sql_src, 'r') as sqlfile:
        query = sqlfile.read()
    query = sqlparse.format(query, strip_comments=True).strip()
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data=data)
    header = ['universidad', 'carrera', 'fecha_de_inscripcion', 'nombre_de_usuario', 'sexo', 'fecha_nacimiento', 'direccion', 'email']
    df.to_csv('/home/lowenhard/airflow/files/ET_Univ_Villa_Maria.csv', header=header, index=False)
    return data

with DAG(
    'python_operator_villa',
    start_date=datetime(2020, 3, 26),
    template_searchpath='/home/lowenhard/airflow/include',
    catchup=False,
    tags=['operator']
) as dag:
    t0 = DummyOperator(task_id='start')
    t1 = PythonOperator(
        task_id='create_csv',
        python_callable=get_data_villa_maria,
        #dag=dag,
    )
    t2 = DummyOperator(task_id='end')

t0 >> t1 >> t2
