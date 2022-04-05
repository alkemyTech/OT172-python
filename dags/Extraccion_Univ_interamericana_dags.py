import os
from airflow import DAG
import datetime
from datetime import datetime, timedelta
from decouple import config
import pathlib
import logging


# Credentials,  path & table id:

TABLE_ID= 'Univ_Interamericana'
PG_ID= config('PG_ID', default='')

# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.
path = (pathlib.Path(__file__).parent.absolute()).parent


# Functions
# Function to define logs, using the logging library: https:/docs.python.org/3/howto/logging.html


def logger(relative_path):
    """Function to configure the code logs

    Args: relativ path to .log file"""
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                        filename=f'{path}/{relative_path}', encoding='utf-8', level=logging.ERROR)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None

# Data extraction function through database queries
# Start connecting to the database through the Postgres operator`s Hook
def extraction(database_id:str, table_id:str):

    """Args: 

    datbase_id: id from the pg-database 
                (previously created 
                from the airflow interface
    table_id:  id of the table to consult

    output: pandas dataframe with queries result
               a csv file with the name of the required table
               is saved locally in the "files" folder 
    

    Extraction function through queries to the database.
    Through the PostgresHook hook (https://airflow.apache.org/,\ndocs/apache-airflow-providers-postgres/stable/\n    _api/airflow/providers/postgres/hooks/postgres/index.html) 
    the connection to a posgreesql database previously created
    in the Airflow interface,taking as a parameter the ID designated 
    to the connection
    """
    logging.info('Connecting to db')
    if (not isinstance(database_id, str) or not isinstance(table_id, str)):
            logging.ERROR('input not supported. Please enter string like values')

    elif ((isinstance(database_id, str) or  isinstance(table_id, str))):
        hook = PostgresHook(postgres_conn_id=database_id)
        conn = hook.get_conn()
        logging.info(f'Conected to {table_id}')

    else:
        logging.error('ERROR: database connection failed')


# SQL query: To execute the query with the Hook, it must be passed as a string to the function
# pd.read_sql, along with the conn object that establishes the connection.
# The .sql file is opened and the text is saved in the query variable
    logging.info('opening sql file')
    if os.path.exists(f'{path}/include/{table_id}.sql'):
        try:        
            with open(str(f'{path}/include/{table_id}.sql')) as file:
                try:
                    query = str(text(file.read()))
                    logging.info(f'Extracting data to {file}')
                except:
                    logging.ERROR('cant read sql file')
        except:
            logging.ERROR('Open .sql file failed')
    else:
            logging.error(f'file not exist')

# The output of this function is a df with the selected rows and columns
# Finally, the df is saved as .csv
    try:
        logging.info('executing query')
        df = pd.read_sql(query, conn)
        logging.info('query successfull')
    except:
        logging.ERROR('query fail')
    df.to_csv(f'{path}/files/{table_id}.csv', sep='\t')
    logging.info('Data saved as csv')
    return None


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
         description='for the execution of an sql query to the trainingn\
          database, to obtain information about Universidad tres de febrero',
         start_date=datetime(2020, 3, 24),
         max_active_runs=3,
         schedule_interval='@hourly',
         default_args=default_args,
         template_searchpath=f'{path}/include',
         catchup=False,
         ) as dag:

# PythonOperator for extraaction function, commented above
    extraction_task = PythonOperator(
        task_id="Extraction",
        python_callable=extraction,
        op_kwargs={'database_id': 'training_db',
                   'table_id': TABLE_ID}
    )
# PythonOperator for logger function, commented above
    logging_task = PythonOperator(
        task_id="logguers",
        python_callable=logger,
        op_args= {f'{path}/logs/{TABLE_ID}'}
    )

    logging_task 
    extraction_task

