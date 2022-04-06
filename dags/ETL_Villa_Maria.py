# run every hour, everyday
# try 5 times

# 1 import modules
from pathlib import Path
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

# creating cvs files
ruta = str(Path().absolute())+'/airflow/OT172-python'
def query_csv():

    pg_hook = PostgresHook(postgres_conn_id='postgres', schema='training')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    villamaria_sql = open(f'{ruta}/include/universidad_nacional_de_villa_maria.sql', 'r')
    
    villamaria_query = villamaria_sql.read()
    
    maria_df = pd.read_sql(villamaria_query, connection)
    maria_df.to_csv(f'{ruta}/files/villa_maria.csv')

# 2 define default arguments
default_args = {
    'retries': 5,  
    'retry_delay': timedelta(minutes=5)
}

# 3 instantiate the DAG
with DAG(
    'ETL_Villa_Maria',
    description = 'ETL for a university in the group A',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,3,30),
    catchup=False
    
) as dag:

# 4 define tasks
    extract = PythonOperator(
                    task_id='Query_Villa_Maria',
                    python_callable=query_csv,
                    op_kwargs={
                        'sql_file': 'query_villa_maria.sql',
                        'file_name': 'villa_maria.csv'
                        }
                    )
    process = DummyOperator(task_id="Process_Data")
    load = DummyOperator(task_id="Load_Data")

# 5 define dependencies
extract >> process >> load