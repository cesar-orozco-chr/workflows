from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

default_args = {
   
}

def _downloading_data():
    print('My first test')


with DAG('simple_dag', start_date=datetime(2021,5,10)) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data 
    )