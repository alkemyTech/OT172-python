from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(filename, key, bucket_name):
    hook = S3Hook('s3-connection')
    #hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    print(filename, key, bucket_name)
    print("Created Connection")
    print(hook.get_session())
    print(hook)

with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'filepath',
            'key': 'filename',
            'bucket_name': 'cohorte-marzo-77238c6a'
        }
    )

    t0 = DummyOperator(task_id='start')
    t1 = task_upload_to_s3

t0 >> t1
