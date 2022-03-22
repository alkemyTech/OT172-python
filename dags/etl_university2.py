from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PostgresOperator, PythonOperator, DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
"""
DAG configuration, without queries or processing for "Universidad De Morón"
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}
with DAG(
    'elt_university1_groupF',
    description = 'etl for group of universities F (Universidad De Morón)',
    schedule_interval = timedelta(hours=1),
    default_args=default_args,
    start_date = datetime(2022, 3, 15)
) as dag:
    query_task1 = PostgresOperator(
        task_id = "Query_Uni1",
        dag = dag
    )
    transformation_task = PythonOperator(
        task_id = "Transformation",
        dag = dag
    )
    load_task = DummyOperator(
        task_id = "Load",
        dag = dag
    )
