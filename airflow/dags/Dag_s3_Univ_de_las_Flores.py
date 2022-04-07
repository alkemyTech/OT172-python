"""
Poder subir el txt creado por el operador de Python al S3

Tomar el .txt del repositorio base
Buscar un operador creado por la comunidad que se adecue a los datos.
Configurar el S3 Operator para la Universidad De Flores
Subir el archivo a S3
"""
import logging
from pathlib import Path
import os

from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

folder = Path(__file__).resolve().parent.parent
log = logging.getLogger(__name__)

def upload_to_s3(filename, key, bucket_name):
    hook = S3Hook('s3-connection')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    has_key = hook.check_for_key(key, bucket_name=bucket_name)
    log.info(f'filename: {filename}, s3 key: {key}')
    log.info('Created Connection: {hook.get_session()}')
    log.info(f'File exists {str(has_key)}')

with DAG(
    dag_id='s3_uni_flores',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': os.path.join(folder, 'files/ET_Univ_DE_Flores.txt'),
            'key': 'ET_Univ_DE_Flores.txt',
            'bucket_name': Variable.get('BUCKET_NAME')
        }
    )

    t0 = DummyOperator(task_id='start')
    t1 = task_upload_to_s3

t0 >> t1
