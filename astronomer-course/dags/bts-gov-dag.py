from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

default_args = {
    'owner': 'cesar',
    'start_date': days_ago(1),
    'schedule_interval': '@daily',
    'retry_delay': timedelta(60),
    'max_active_runs': 1
}


def _bash_command(script: str):
    return (f"bash $PYTHONPATH/dags/scripts/{script} "
            "{{ params.year }} {{ params.month }} {{ params.file_path }}")


def bulk_data_into_bts_table(**kwargs):

    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:postgres@192.168.20.31:5433/postgres')
    df = pd.read_csv(f"{kwargs['params']['file_path']}/{kwargs['params']['year']}{kwargs['params']['month']}.csv")
    df.columns = [c.lower() for c in df.columns]
    df2 = df.head(30)
    df2.to_sql(f"bts_gov_{kwargs['ds_nodash']}", engine, if_exists='replace')


with DAG('bts-gov',
         default_args=default_args,
         params={'year': 2015,
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

    bulk_data_into_bts_table = PythonOperator(
        task_id='bulk_data_into_bts_table',
        python_callable=bulk_data_into_bts_table,

    )

download_data >> wait_for_data >> zip_to_csv >> clean_up_zip >> quotes_comma >> bulk_data_into_bts_table
