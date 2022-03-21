# The goal is to configure the retry for the tasks
# DAG of the following universities:
# Universidad Nacional de La Pampa
# Universidad Interamericana

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models.connection import Connection

c = Connection(
    conn_id="some_conn",
    conn_type="mysql",
    description="connection description",
    host="http://training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com",
    login="alkymer",
    password="alkymer123",
    schema= "training"
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# The database was configured from Airflow, entering the option
# "connections" of the "Admin" tab and the designated ID, will be
# passed as a parameter to the "PostgreOperator" class.

with DAG('172-16',
         start_date=datetime(2020, 3, 20),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath='/home/juan/airflow/include',
         catchup=False,
         ) as dag:

    opr_LaPampa = PostgresOperator(
        task_id='LaPampa',
        postgres_conn_id='some_conn',
        sql='sql-Univ_nac_LaPampa.sql'
    )

    opr_Interamericana = PostgresOperator(
        task_id='Interam',
        postgres_conn_id='some_conn',
        sql='sql-Univ-Interamericana.sql'
    )

    opr_LaPampa
    opr_Interamericana
