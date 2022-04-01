from asyncio import Task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
from airflow import settings
from sympy import Id
import pandas as pd
import datetime
from datetime import datetime
from settings import *
import logging
import os
import pathlib


# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path = (pathlib.Path(__file__).parent.absolute()).parent

# Function to define logs, using the logging library: https:/docs.python.org/3/howto/logging.html


def logger():
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                        filename=f'{__file__}/logs/LaPampa_univ_logs.log', encoding='utf-8', level=logging.DEBUG)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None

# Function to create/establish the connection to the database.
# Initially, the function looks for a database with the id. # if ID exist, establish a connection. If not, the try block fails.
# and the exception is raised, creating a new connection


def get_connection(username, password, host, db, conntype, id, port):
    try:
        hook = PostgresHook(postgres_conn_id=id)
        conn = (hook.get_uri())
        logging.info(f'Conected to {id}')
    except:
        logging.info(f'Creating connection to {id}')
        new_conn = Connection(conn_id=id, conn_type=conntype,
                              login=username,
                              host=host, schema=db, port=int(port))
        new_conn.set_password(password)

        session = settings.Session()

        session.add(new_conn)
        session.commit()
        logging.info(f'Conected to {id}')


# Data extraction function through database queries
# Start connecting to the database through the Postgres operator`s Hook
def extraction():
    try:
        import pandas as pd
        from sqlalchemy import text
        hook = PostgresHook(postgres_conn_id='training_db')
        conn = hook.get_conn()
        logging.info(f'Conected to {id}')
    except:
        logging.error(f'Conection Error')

# SQL query: To execute the query with the Hook, it must be passed as a string to the function
# pd.read_sql, along with the conn object that establishes the connection.
# The .sql file is opened and the text is saved in the query variable
    with open(str(f'{path}/include/Univ_nac_LaPampa.sql')) as file:
        try:
            query = str(text(file.read()))
            logging.info(f'Extracting query to {file}')
        except:
            logging.error(f'Error')

# The output of this function is a df with the selected rows and columns
# Finally, the df is saved as .csv
    df = pd.read_sql(query, conn)
    logging.info('qury successfull')
    df.to_csv(f'{path}/files/ET_Univ_nac_LaPampa.csv', sep='\t')
    logging.info('Data saved as csv')
    return(df)


# Retries configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# Dag definition for the E process
with DAG('Extraction_Univ_LaPamPa',
         description='for the execution of an sql query to the training database, to obtain information about Universidad tres de febrero',
         start_date=datetime(2020, 3, 24),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path}/include',
         catchup=False,
         ) as dag:

# PythonOperator for the execution of get_connection, commented above
    connect_to_db = PythonOperator(
        task_id="connection",
        python_callable=get_connection,
        op_kwargs={'username': USER, 'password': PASSWORD,
                   'db': SCHEMA, 'host': HOST,
                   'conntype': CONNECTION_TYPE, 'id': ID, 'port': PORT}

    )

# PythonOperator for extraaction function, commented above
    extraction_task = PythonOperator(
        task_id="Extraction",
        python_callable=extraction
    )
# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger
    )

    connect_to_db >> logging_task >> extraction_task
