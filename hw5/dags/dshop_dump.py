import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


from datetime import datetime
from hdfs import InsecureClient


def dshop_dump_table_task(table_to_dump, **kwargs):
    hdfs_client = InsecureClient(**Variable.get('HDFS_CREDENTIALS', deserialize_json=True))
    bronze_path = f'/bronze/dshop_dump/{kwargs["ds"]}'
    hdfs_client.makedirs(bronze_path)

    with psycopg2.connect(**Variable.get('POSTGRES_CREDENTIALS', deserialize_json=True)) as pg_connection:
        cursor = pg_connection.cursor()
        with hdfs_client.write(f'{bronze_path}/{table_to_dump}.csv', overwrite=True) as csv_file:
            cursor.copy_expert(f'COPY {table_to_dump} TO STDOUT WITH HEADER CSV', csv_file)


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

with DAG(
        dag_id="dshop_dump_dag",
        description='Dump data from dshop database in postgresql',
        schedule_interval='@daily',
        start_date=datetime(2022, 2, 7, 1, 0),
        default_args=default_args
) as dag:
    tasks = []
    tables = ('aisles', 'clients', 'departments', 'orders', 'products')

    for table in tables:
        tasks.append(
            PythonOperator(
                task_id=f'dshop_dump_table_{table}_task',
                python_callable=dshop_dump_table_task,
                provide_context=True,
                op_kwargs={'table_to_dump': table}
            )
        )

    start = DummyOperator(task_id='dshop_dump_start_task')
    end = DummyOperator(task_id='dshop_dump_end_task')

    start >> tasks >> end
