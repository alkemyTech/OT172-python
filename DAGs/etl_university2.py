from datatime import datatime, timedelta
from airflow import DAG
from airflow.operators import PostgresOperator, PythonOperator, DummyOperator

"""
DAG configuration, without queries or processing for "Universidad De Morón"
"""
with DAG(
    'elt_university1_groupF',
    description = 'etl for group of universities F (Universidad Nacional De Río Cuarto)',
    schedule_interval = timedelta(hours=1),
    start_date = datatime(2022, 3, 15)
) as dag:
    query_task1 = PostgresOperator(
        task_id = "Query_Uni2",
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