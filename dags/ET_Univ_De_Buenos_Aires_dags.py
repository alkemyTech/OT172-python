"""
Configurar el DAG para procese las siguientes universidades:
Universidad De Buenos Aires
Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta que se va a hacer dos consultas SQL
(una para cada universidad), se van a procesar los datos con pandas y se van a cargar los datos en S3.
El DAG se debe ejecutar cada 1 hora, todos los días.
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    'dag_univ_buenos_aires',
    start_date=datetime(2022, 3, 23),
    schedule_interval=timedelta(days=1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    @task(task_id='')
    def get_data_univ_uba(query):
        postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()

    @task(task_id='')
    def process_data():
        """ Porcess data with pandas """
        pass

    @task(task_id='')
    def upload_to_s3():
    """ Upload data to s3 """
        pass

     get_data_univ_uba() >> process_data() >>  upload_to_s3()
