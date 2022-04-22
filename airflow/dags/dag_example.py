from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

default_args={
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

def foo():
    log.info('foo here')

with DAG(
    'hello_dag',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description='A dummy DAG that prints some info',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 3),
    catchup=False,
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_info',
        bash_command="echo Starting...",
        params=default_args,
    )

    @task(task_id="print_the_context")
    def print_context():
        """Print the Airflow context and ds variable from the context."""
        log.info('This is a test')
        return 'Whatever you return gets printed in the logs'
    run_this = print_context()

    t2 = PythonOperator(
        task_id='pprint',
        python_callable= foo,
        #op_kwargs = {"x" : "Apache Airflow"},
        dag=dag,
    )

    t3 = BashOperator(
        task_id='print_final',
        depends_on_past=False,
        bash_command='echo "Finish..."',
    )

    dag.doc_md = 'Dag doc to take into account'
    t3.doc_md = 'Task doc to take into account'

    t1 >> t2 >> run_this >> t3
