from datetime import datetime
import os
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

pg_creds = {
    'host': '192.168.31.7',
    'port': '5432',
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}


def dshop_dump_task(**kwargs):
    tables = ('aisles', 'clients', 'departments', 'orders', 'products')
    folder = f'/home/user/dshop_dump/{kwargs["ds"]}'
    os.makedirs(folder, exist_ok=True)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        for table in tables:
            with open(file=os.path.join(folder, f'{table}.csv'), mode='w') as csv_file:
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
