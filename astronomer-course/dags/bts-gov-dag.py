from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

default_args = {
    'owner': 'cesar',
    'start_date': datetime(2021,5,11),
    'schedule_interval': '@daily',
    'retry_delay': timedelta(60)
}

def _bash_command(script: str):
    return (f"$PYTHONPATH/dags/scripts/{script} "
        "{{ params.year }} {{ params.month }} {{ params.file_path }}")

with DAG('bts-gov',
        default_args=default_args,
        params={'year':2015, 
        'month': 12,
        'file_path': '/tmp'},
        catchup=False) as dag:
    
    download_data = BashOperator(
        task_id='download_data',
        bash_command=_bash_command("download_file.sh")
    
    )

    wait_for_data = FileSensor(
        task_id='wait_for_data',
        fs_conn_id='fs_default',
        filepath="{{ params.year }}{{ params.month }}.zip"
    )

    zip_to_csv = BashOperator(
        task_id='zip_to_csv',
        # bash_command=("${PYTHONPATH}/dags/scripts/zip_to_csv.sh "
        # "{{ params.year }} {{ params.month }} {{ params.file_path }}")
        bash_command=_bash_command("zip_to_csv.sh")
    )

    clean_up_zip = BashOperator(
        task_id='clean_up_zip',
        bash_command="rm -rf {{ params.file_path }}/{{ params.year }}{{ params.month }}.zip"
    )

    quotes_comma = BashOperator(
        task_id='quotes_comma',
        bash_command=_bash_command("quotes_comma.sh")
    )

    
    bulk_data_into_bts_table = PostgresOperator(
        task_id='bulk_data_into_bts_table',
        postgres_conn_id="postgres_default",
        sql="sql/copy_bts_data.sql",
        params={
            "csv_file": "{{ params.file_path }}/{{ params.year }}{{ params.month }}.csv"
        }

    )
    

download_data >> wait_for_data >> zip_to_csv >> clean_up_zip >> quotes_comma >> bulk_data_into_bts_table