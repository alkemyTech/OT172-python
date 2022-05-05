import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def Etl():
    @task
    def get_data():
        query = """
            select
                universidades, carreras, fechas_de_inscripcion, nombres, sexo, fechas_nacimiento, codigos_postales
            from
                uba_kenedy uk
            where
                TO_DATE(fechas_de_inscripcion, 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
                and universidades = 'universidad-de-buenos-aires'
            ;
        """
        postgres_hook = PostgresHook(postgres_conn_id="airflow-universities")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()

    @task
    def process_data():
        pass

    @task
    def save_data_to_s3():
        pass

    get_data() >> process_data() >> save_data_to_s3()


dag = Etl()
