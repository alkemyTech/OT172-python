from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc
from decouple import config
import pathlib
import logging
import pandas as pd

class QueryExecuteReturnNothing(Exception):
    def __init__(self, path_folder, file_query):
        self.path_folder = path_folder
        self.file_query = file_query
        super().__init__("error, query return nothing")

# configuracion del logging
# Formato del log: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d')
## Por si necesitamos que los logs tengan el nombre del archivo donde se encuentra
logger = logging.getLogger(__name__)

#Configuracion de parametros de .env (luego en unas variables de airflow)
user = config("USER")
password = config("PASSWORD")
# eliminar http/https si esta en el host 
host = config("HOST")
port = config("PORT")
db = config("DB")
db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

def read_query_to_csv(**kwargs):
    filename = kwargs['filename']
    filename += '.sql' if filename[-4:] not in '.sql' else ""
    folder_path = kwargs['folder_path']
    path_download = kwargs['path_download']
    url = kwargs['db_url']

    logger.info(f"leyendo la query {filename}, en la ruta {folder_path}")
    try:
        with open(str(folder_path) + str(filename), 'r') as f:
            query = f.read()
    except FileNotFoundError as e:
        logger.error(f"""
        FileNotFoundError: el archivo en la ruta {e.filename}
        no existe o no se puede leer.""")
        raise AirflowException("Fallo a realizar la funcion: read_query_to_csv")
    except (OSError, IOError) as e:
        logger.error(e)
        raise AirflowException("Fallo a realizar la funcion: read_query_to_csv")

    logger.info("leida la query del archivo: " + filename)
    logger.info("procediendo con la consulta sql a la base de datos")
    # conectarse a la base de datos con los parametros
    try:
        datas = create_engine(url).execute(query)
        if not datas:
            raise QueryExecuteReturnNothing(folder_path, filename) 
        df = pd.DataFrame(datas, columns=datas.keys())
        df.to_csv(path_download + filename + '.csv', index=False)
    except QueryExecuteReturnNothing as e:
        logger.warning(f"""
        QueryExecuteReturnNothing: la query {e.file_query} en la ruta
        {e.folder_path}, retorno vacio. Por favor revisar la query en el .sql
        """)
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
    logger.info(f"Resultado de la query {filename} guardado en: \n {path_download}")


""" 
Configuracion del DAG
Configurar un DAG, sin consultas, ni procesamiento para el grupo de universidades G:
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
with DAG('ET_Uni_J_F_Kennedy', 
        default_args=default_args,
        catchup=False
        ) as dag:

        extract_sql_query = PythonOperator(
            task_id='extract_sql_universidad_j_f_kennedy',
            python_callable=read_query_to_csv,
            op_kwargs={'filename': 'Uni_J_F_Kennedy.sql', 
                        'folder_path': str(pathlib.Path().absolute()) + '/include/',
                        'path_download': str(pathlib.Path().absolute()) + '/files/',
                        'db_url': db_url},
            provide_context=True,
            retries=5,
            retry_delay=timedelta(minutes=1),
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

        extract_sql_query >> transform_pd >> load_s3
