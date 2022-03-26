from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pathlib
import logging
import pandas as pd


# configuracion del logging
# Formato del log: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d')
## Por si necesitamos que los logs tengan el nombre del archivo donde se encuentra
logger = logging.getLogger(__name__)

#Configuracion de parametros de .env (luego en unas variables de airflow)
user = "alkymer"
password = "alkymer123"
host = "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com"
port = "5432"
db = "training"
URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

#Funcion que se conecta a una base de datos con postgresHook 
# lee una query en un archivo y la ejecuta en la base de datos
# descargando los datos en un archivo csv
def read_query_to_csv(**kwargs):
    # leer el archivo
    #FileNotFoundError
    with open(str(pathlib.Path().absolute()) + '/include/' + kwargs['filename'] + '.sql', 'r') as f:
        query = f.read()
    logger.info("leida la query")
    # conectarse a la base de datos con los parametros
    datas = create_engine(URL).execute(query).fetchall()
    logger.info("conectado a la base de datos")
    #importar los datos a un archivo csv con pandas
    df = pd.DataFrame(datas)

    # raise OSError(fr"Cannot save file into a non-existent directory: '{parent}'")
    # check_parent_directory 
    # https://stackoverflow.com/questions/47143836/pandas-dataframe-to-csv-raising-ioerror-no-such-file-or-directory
    
    df.to_csv(str(pathlib.Path().absolute()) + '/csv_files/' + kwargs['filename'] + '.csv')
    logger.info("exportado a csv")



    



""" 
Configuracion del DAG
Configurar un DAG, sin consultas, ni procesamiento para el grupo de universidades G:
  Facultad Latinoamericana De Ciencias Sociales
  Universidad J. F. Kennedy 
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), #parametro general de intentos para todas las tareas
    'start_date': datetime(2019, 1, 1),
    'schedule_interval': '0 * * * *',
    'execution_timeout': timedelta(minutes=30), #luego de 30m falla en cualquier tarea
    'trigger_rule': 'all_success'
}

"""
   Decision de diseño: Esto es una ETL por lo:
   Extract: seran el conjunto de las task que realizaran la ejecucion de los .sql
   Transform: sera la task que ejecutara una funcion, objeto instanciado, etc de pandas
        o de cualquier metodo que usemos para procesar los datos
   Load: la task que importara los datos limpiados y enriquecidos a S3.
"""
# Ejemplo de juguete del minimo (por ahora) de task y operators necesarias.
with DAG('ET_Fac_latinoamericana_Ciencias_Sociales', 
        default_args=default_args,
        catchup=False
        ) as dag:

        # reintentar 5 veces la tarea, si es que falla
        # Configurar un Python Operators, para que extraiga información de la base de datos utilizando el .sql disponible en el repositorio base de las siguientes universidades: 
        extract_sql_query_1 = PythonOperator(
            task_id='extract_sql_query_1',
            python_callable=read_query_to_csv,
            # podriamos agregar el path del archivo .sql
            op_kwargs={'filename': 'facultad_latinoamericana_de_ciencias_sociales'},
            provide_context=True,
            dag=dag
        )

        transform_pd = BashOperator(
            task_id='transform_pd',
            bash_command='echo "Transformando a dataframe"',
            dag=dag
        )

        load_s3 = BashOperator(
            task_id='load_s3',
            execution_timeout=timedelta(minutes=3),
            retries=1,
            retry_delay=timedelta(minutes=2),
            bash_command='echo "Cargando a S3"',
            dag=dag
        )

        transform_pd >> extract_sql_query_1>> load_s3