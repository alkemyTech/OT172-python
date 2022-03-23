from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pathlib
import logging


# configuracion del logging
# Formato del log: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d')
## Por si necesitamos que los logs tengan el nombre del archivo donde se encuentra
# logger = logging.getLogger(__name__)

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
with DAG('etl_universidades_latCienciasSociales_JFKennedy', 
        default_args=default_args,
        template_searchpath = str(pathlib.Path().absolute()) + '/../include',
        catchup=False
        ) as dag:

        # reintentar 5 veces la tarea, si es que falla
        extract_sql_query_1 = BashOperator(
            task_id='extract_sql_query_1',
            execution_timeout=timedelta(minutes=3),
            retries=5,
            retry_delay=timedelta(minutes=2),
            bash_command='echo "Ejecutando query 1, Facultad Latinoamericana De Ciencias Sociales"',
            dag=dag
        )

        extract_sql_query_2 = BashOperator(
            task_id='extract_sql_query_2',
            execution_timeout=timedelta(minutes=3),
            retries=5,
            retry_delay=timedelta(minutes=2),
            bash_command='echo "Ejecutando query 2, Universidad J. F. Kennedy"',
            dag=dag
        )
        
        """
            se usa una sola task para el procesamiento de los datos de las
            dos universidades, hay problemas duplicar las tareas de 
            procesamiento de datos, se subida a S3 y luego ordenar el flujo.
        """

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

        extract_sql_query_1 >> transform_pd >> load_s3
        extract_sql_query_2 >> transform_pd >> load_s3



         