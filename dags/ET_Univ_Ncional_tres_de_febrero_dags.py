from asyncio import Task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
from TXT_tOs3oPERATOR import *

# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path = (pathlib.Path(__file__).parent.absolute()).parent

# root
# Function to define logs, using the logging library: https://docs.python.org/3/howto/logging.html


def logger():
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                        filename=f'{path}/tres_de_febrero_univ_logs.log', encoding='utf-8', level=logging.DEBUG)
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
                              host=host, schema=db, port=port)
        new_conn.set_password(password)

        session = settings.Session()

        session.add(new_conn)
        session.commit()
        logging.info(f'Conected to {id}')


# Data extraction function through database queries
# Start connecting to the database through the Postgres operator`s Hook
def extraction():
        import pandas as pd
        from sqlalchemy import text
        hook = PostgresHook(postgres_conn_id='training_db')
        conn = hook.get_conn()
      
# SQL query: To execute the query with the Hook, it must be passed as a string to the function
# pd.read_sql, along with the conn object that establishes the connection.
# The .sql file is opened and the text is saved in the query variable
        with open(f'{path}/include/Univ_nacional_tres_de_febrero.sql') as file:
            try:
                query = str(text(file.read()))
                logging.info(f'Extracting query to {file}')
            except:
                logging.error(f'Error')


# The output of this function is a df with the selected rows and columns
# Finally, the df is saved as .csv
        df = pd.read_sql(query, conn)
        return(df)

# Function that removes spaces at the beginning or end of strings, hyphens and
# convert words to lower case


def normalize_characters(column):
    column = column.apply(lambda x: str(
        x).replace(' \W'+'*'+'\W', '\W'+'*'+'\W'))
    column = column.apply(lambda x: str(
        x).replace('\W'+'*'+'\W ', '\W'+'*'+'\W'))
    column = column.apply(lambda x: str(x).replace('-', ' '))
    column = column.apply(lambda x: str(x).replace('_', ' '))
    column = column.apply(lambda x: x.lower())
    return column

# Data transformation function

# lines 1-3: apply the function previously defined
# line : conditional that changes m and f to male and female, in the gender column
# lines 5-6: The string contained in inscription date is passed to date format
# and the required format is assigned
# lines 7-8: Using the split function, the values ​​of the name column are separated
# and the output is split into first_name and last_name columns
# lines 9-11: The format of the birth`s date is (DD/MM/YY), when passing the
# string to date, the program did not differentiate between the decades that belonged to 1900
# and those of 2000. To obtain the age, the year was extracted from the current date, the last two characters of the date were taken,
# step to number and the age was obtained using the formula 100+(current date - date of birth)
# lines 12-16: A csv with the postal codes and their corresponding cities was passed to df.
# the names of the cities were changed to lowercase letters to match the localities in the table
# query. The resulting df was passed to the dictionary, establishing the postl code variable as key (also defined in the
# sql query table. Finally, the values ​​of the zip codes in the query table are called, in the dictionary
# previously defined, resulting in the corresponding localities column).
# line 17: only the required columns were selected


def transformation(df):
    path=(pathlib.Path(__file__).parent.absolute()).parent
    logging.info(f'normalizing data')
    df['university'] = normalize_characters(df['university'])
    df['career'] = normalize_characters(df['career'])
    df['gender'] = df['gender'].apply(
        lambda x: 'male' if x == 'm' else 'female')

    old_date = pd.to_datetime(df['inscription_date'])
    df['inscription_date'] = pd.to_datetime(old_date, '%Y/%m/%d')

    df['first_name'] = df['name'].apply(lambda x: str(x).split('_')[0])
    df['last_name'] = df['name'].apply(lambda x: str(x).split('_')[1])

    curr = datetime.now()
    df['age'] = df['nacimiento'].apply(lambda x: (
        100+(int(str(curr.year)[2:4]) - int(x[7:9]))))

    df_postal_codes = (pd.read_csv(f'{path}/dataset/codigos_postales.csv'))
    df_postal_codes['localidad'] = df_postal_codes['localidad'].apply(
        lambda x: x.lower())
    dict_postal_codes = dict(
        zip(df_postal_codes.codigo_postal, df_postal_codes.localidad))
    df['location'] = df['postal_code'].apply(lambda x: dict_postal_codes[int(x)])

    df = df[['university', 'career', 'inscription_date', 'first_name',
             'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    df.to_csv(f'{path}/files/ET_Univ_nacional_tres_de_febrero.txt', sep='\t')
    return(df)

# Function for the entire ETL process, which will be called through a PythonOperator


def ET_function(**kwargs):
        df = extraction()
        logging.info('Extraction successful')
        df_t = transformation(df)
        

# Retries configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# Dag definition for the ETL process
with DAG('ETl_Univ_nacional_tres_de_febrero',
         start_date=datetime(2020, 3, 24),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path}/airflow/include',
         catchup=False,
         ) as dag:

    # PythonOperator for the execution of get_connection, commented above
    connect_to_db = PythonOperator(
        task_id="connection",
        python_callable=get_connection,
        op_kwargs={'username': LOGIN, 'password': PASSWORD,
                   'db': SCHEMA, 'host': HOST,
                   'conntype': 'HTTP', 'id': ID, 'port': PORT}

    )

# PythonOperator for ETL function, commented above
    ET_task = PythonOperator(
        task_id="ET",
        python_callable=ET_function
    )

# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger
    )

    logging_task >> connect_to_db >> ET_task