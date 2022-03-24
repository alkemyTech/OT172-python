from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config as configuracion
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os 
import logging

#Configuracion de parametros de .env
Usuario = configuracion("pguser")
Contra = configuracion("pgpass")
Host = configuracion("pghost")
Puerto = configuracion("pgport")
NombreDB = configuracion("pgdb")
URL = f"postgresql://{Usuario}:{Contra}@{Host}/{NombreDB}"
#Funcion inicial de logging
def log_function():
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='logs.log', encoding='utf-8', level=logging.DEBUG)
    return None
#Funcion de conexion a DB
def Conexion(**kwargs):
    try:
        conexion = create_engine(kwargs['URL'], pool_size=50, echo=False)
        logging.info("Conexion a base de datos realizada con exito.")
        ruta_absoluta = os.getcwd()
        ruta_sql = ruta_absoluta + "/airflow/include/Uni_Nacional.sql" 
        with open(ruta_sql, 'r', encoding="utf8") as ArchivoSQL:
            SQL_Data = ArchivoSQL.read()
            Datos = conexion.execute(SQL_Data).fetchall()
            Dataframe = pd.DataFrame(Datos)
        logging.info("Consulta realizada con exito.")
        Dataframe.to_csv(ruta_absoluta + "/airflow/files/Uni_Nacional.csv", index=False, encoding="utf-8")
        logging.info("Dataframe Universidad Nacional de Rio Cuarto exportado a csv con exito.")
        return None
    except Exception as e:
        logging.error(e)
"""
DAG configuration, without queries or processing for "Universidad Nacional"
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
    description = 'etl for group of universities F (Universidad Nacional de Río Cuarto)',
    schedule_interval = timedelta(hours=1),
    default_args=default_args,
    start_date = datetime(2022, 3, 15)
) as dag:
    log_conf = PythonOperator(
        task_id = "Logs_configuration",
        python_callable = log_function
    )
    query_task = PythonOperator(
        task_id = "Query_Uni_Moron",
        python_callable = Conexion,
        op_kwargs={
            'URL':URL
        }
    )
    log_conf >> query_task