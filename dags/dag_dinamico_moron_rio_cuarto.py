# Este archivo es el generador de los dags
# Universidad de Moron
# universidad Nacional de Rio Cuarto
# lee los datos del archivo dag_dinamico_moron_rio_cuarto.yml

from airflow import DAG
import dagfactory


config_file = "/home/tackel/apache-airflow-aceleracion/airflow/dags/OT172-python/dags/dag_dinamico_moron_rio_cuarto.yml"
example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
