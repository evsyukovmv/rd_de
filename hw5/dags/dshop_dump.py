import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
from hdfs import InsecureClient


pg_creds = {
    'host': '192.168.31.7',
    'port': '5432',
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}

hdfs_creds = {
    'url': 'http://192.168.31.7:50070/',
    'user': 'user'
}


def dshop_dump_task(**kwargs):
    tables = ('aisles', 'clients', 'departments', 'orders', 'products')

    hdfs_client = InsecureClient(**hdfs_creds)
    bronze_path = f'/bronze/dshop_dump/{kwargs["ds"]}'
    hdfs_client.makedirs(bronze_path)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        for table in tables:
            with hdfs_client.write(f'{bronze_path}/{table}.csv') as csv_file:
                cursor.copy_expert(f'COPY {table} TO STDOUT WITH HEADER CSV', csv_file)


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    dag_id="dshop_dump_dag",
    description='Dump data from dshop database in postgresql',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 7, 1, 0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id="dshop_dump_task",
    python_callable=dshop_dump_task,
    provide_context=True,
    dag=dag
)
