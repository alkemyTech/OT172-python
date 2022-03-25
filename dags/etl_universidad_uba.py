"""
PT172-35
Configurar un DAG, sin consultas, ni procesamiento
Configurar el DAG para procese las siguientes universidades:
Universidad Del Cine
Universidad De Buenos Aires
Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta que se va a hacer
dos consultas SQL (una para cada universidad), se van a procesar los datos con pandas y se van a
cargar los datos en S3.  El DAG se debe ejecutar cada 1 hora, todos los días.
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def get_data_uba():
    query = """
        select
            universidades, carreras, fechas_de_inscripcion, nombres, sexo, fechas_nacimiento, codigos_postales
        from
            uba_kenedy uk
        where
            TO_DATE(fechas_de_inscripcion, 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
            and universidades = 'universidad-de-buenos-aires'
        limit 2
        ;
    """
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()

@task
def get_data_cine():
    query = """
        select
            universities, careers, inscription_dates, names, sexo, birth_dates, locations
        from
            lat_sociales_cine
        where
            TO_DATE(inscription_dates, 'DD-MM-YYYY') between '2020-09-01' and '2021-02-01'
            and universities = 'UNIVERSIDAD-DEL-CINE'
        limit 2
        ;
    """
    postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()

with DAG(
    'postgres_dag',
    #dag_id="postgres_operator_dag",
    start_date=datetime(2022, 3, 23),
    schedule_interval=timedelta(days=1),
    #schedule_interval="@once",
    catchup=False,
) as dag:
    get_data_cine() >> get_data_uba()
