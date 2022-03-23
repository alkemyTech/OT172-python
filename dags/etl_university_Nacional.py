from datetime import datetime, timedelta
from airflow import DAG
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
    'elt_university_Nacional',
    description = 'etl for group of universities F (Universidad Nacional De Río Cuarto)',
    schedule_interval = timedelta(hours=1),
    default_args=default_args,
    start_date = datetime(2022, 3, 15)
) as dag:
    query_task1 = DummyOperator(
        task_id = "Query_Uni1",
        dag = dag
    )
    transformation_task = DummyOperator(
        task_id = "Transformation",
        dag = dag
    )
    load_task = DummyOperator(
        task_id = "Load",
        dag = dag
    )
