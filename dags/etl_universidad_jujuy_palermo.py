# DAG, no consultation, no processing. For ETL of 2 universities (Jujuy and Palermo)
# The dag runs every 1 hour
# Operators to use: PythonOperator, PostgresOperator

# 


from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    'etl_universidades_jujuy_palermo',
    description = 'ETL dag for 2 universities',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,20),
) as dag:
    def extract():
        pass
    def process():
        pass
    def load():
        pass
    # Coloco en operador a modo de ejemplo
    extract_task = PythonOperator(
        task_id = 'extract',
        python_callable = extract
        )
    process_task = PythonOperator(
        task_id = 'process',
        python_callable = process
        )
    load_task = PythonOperator(
        task_id = 'load',
        python_callable = load
        )


    extract_task >> process_task >> load_task


