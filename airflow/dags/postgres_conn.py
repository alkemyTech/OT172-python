from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

#import os
#import boto3

def s3connection(body):
    """Step 2: The new session validates your request and directs it to your Space's specified endpoint using the AWS SDK."""
    session = boto3.session.Session()
    client = session.client(
                 's3',
                 endpoint_url='https://nyc3.digitaloceanspaces.com', # Find your endpoint in the control panel, under Settings. Prepend "https://".
                 region_name='nyc3', # Use the region in your endpoint.
                 aws_access_key_id='EHFSNR3DT74ZF6LAOJO2', # Access key pair. You can create access key pairs using the control panel or API.
                 aws_secret_access_key=os.getenv('secret-s3') # Secret access key defined through an environment variable.
    )

    # Step 3: Call the put_object command and specify the file to upload.
    client.put_object(Bucket='universidades/universities/', # The path to the directory you want to upload the object to, starting with your Space name.
                Key='hello-world.txt', # Object key, referenced whenever you want to access this file later.
                Body=body, # The object's contents.
                ACL='private', # Defines Access-control List (ACL) permissions, such as private or public.
    )

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
