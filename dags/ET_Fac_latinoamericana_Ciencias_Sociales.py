from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc
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

# read_query_to_csv
# input: key 'filename' in **kwargs
# output: none
# realiza la conexion a una base de datos con parametros pasador por 
# un archivo .env y realiza la query de un archivo .sql, finalmente
# guarda el resultado en un archivo .csv
def read_query_to_csv(**kwargs):
    path_this_file = str(pathlib.Path().absolute())
    filename = kwargs['filename']
    try:
        with open(path_this_file + '/include/' + filename + '.sql', 'r') as f:
            query = f.read()
    except FileNotFoundError as e:
        logger.error(f"""
        FileNotFoundError: el archivo en la ruta {e.filename}
        no existe o no se puede leer.""")
        raise AirflowException("Fallo a realizar la funcion: read_query_to_csv")
    except (OSError, IOError) as e:
        logger.error(e)
        raise AirflowException("Fallo a realizar la funcion: read_query_to_csv")

    logger.info("leida la query del archivo: " + filename + ".sql")
    logger.info("procediendo con la consulta sql a la base de datos")
    # conectarse a la base de datos con los parametros
    try:
        # Como verificar si se retorna algo vacio?
        datas = create_engine(URL).execute(query).fetchall()
        df = pd.DataFrame(datas)
        df.to_csv(path_this_file + '/csv_files/' + filename + '.csv')
    except exc.ObjectNotExecutableError as e:
        logger.error(f"""
        ObjectNotExecutableError: contenido del archivo sql {filename}
        no es una query válida.""")
        raise AirflowException("Fallo realizar la funcion: read_query_to_csv")
    except exc.SQLAlchemyError as e:
        logger.error(f"""
        SQLAlchemyError: fallo la conexion a la base de datos:{URL}""")
        raise AirflowException("Fallo realizar la funcion: read_query_to_csv")
    except (OSError, IOError) as e:
        logger.error(f"""
        OSError: No se puede guardar csv por que la direccion no existe:
        {e}""")
        raise AirflowException("Fallo realizar la funcion: read_query_to_csv")
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