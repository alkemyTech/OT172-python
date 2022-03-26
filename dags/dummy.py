"""
Configurar el DAG para procese las siguientes universidades:
Universidad Del Cine
Universidad De Buenos Aires

Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta
que se va a hacer dos consultas SQL (una para cada universidad), se van a procesar
los datos con pandas y se van a cargar los datos en S3.
El DAG se debe ejecutar cada 1 hora, todos los días.
"""

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

with DAG(
    'etl_universidades_cine_uba',
    description = 'DAG de extraccion de 2 universidades',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022, 3, 22),
) as dag:
    # Por hacer: colocar aquí los operadores

