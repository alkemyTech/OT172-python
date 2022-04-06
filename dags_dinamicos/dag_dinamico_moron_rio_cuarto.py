# Este archivo es el generador de los dags
# Universidad de Moron
# universidad Nacional de Rio Cuarto
# lee los datos del archivo dag_dinamico_moron_rio_cuarto.yml

from airflow import DAG
import dagfactory
from os import path

ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))

config_file = f"{ruta_base}/dags_dinamicos/dag_dinamico_moron_rio_cuarto.yml"
dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
