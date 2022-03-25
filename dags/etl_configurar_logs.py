"""
PT-172-51
Configurar logs para Universidad Del Cine
Configurar logs para Universidad De Buenos Aires
Utilizar la librería de logging de python: https://docs.python.org/3/howto/logging.html
Realizar un log al empezar cada DAG con el nombre del logger
Formato del log: %Y-%m-%d - nombre_logger - mensaje
Aclaración:
Deben dejar la configuración lista para que se pueda incluir dentro de las funciones futuras.
No es necesario empezar a escribir logs.
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

FORMAT = '%(fecha) %(nombre) %(mensaje)'
logging.basicConfig(filename='dags.log', encoding='utf-8', level=logging.INFO, format=FORMAT)
log = logging.getLogger("logs-universidades")

def logs_universidad_del_cine():
    """ Produce los logs de la universidad del cine """
    pass

def logs_universidad_uba():
    """ Produce los logs de la universidad de Buenos Aires """
    pass

default_args = {
    'owner': 'Leonardo',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 25),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'logs-universidades',
    schedule_interval='0 * * * *',
    start_date = datetime(2022, 3, 25),
    default_args=default_args
    ) as dag:
        log_ucine = PythonOperator(
            python_callable=logs_universidad_del_cine,
            task_id='log_universidad_del_cine'
        )

        log_uba = PythonOperator(
            python_callable=logs_universidad_ubae,
            task_id='log_universidad_uba'
        )

log_ucine >> log_uba
