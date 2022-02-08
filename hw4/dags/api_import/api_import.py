import sys
import os
sys.path.append(os.path.join('/home/user/airflow/dags/api_import/'))

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from api_client.config import Config
from api_client.api import Api
from api_client.storage import Storage


def api_import_task(**kwargs):
    config = Config.load(os.path.join('/home/user/airflow/dags/api_import/api_client/config.yaml'), 'robot_dreams_de')
    api = Api(config['api'])
    storage = Storage(config['storage'], 'out_of_stock')
    date = kwargs['ds']

    print(date)

    print('\tRetrieving data ')
    data = api.out_of_stock(date)

    print('\tSaving data')
    storage.save(date, data)


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    dag_id="api_import_dag",
    description='Dump data from dshop database in postgresql',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 7, 1, 0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id="api_import_task",
    python_callable=api_import_task,
    provide_context=True,
    dag=dag
)
