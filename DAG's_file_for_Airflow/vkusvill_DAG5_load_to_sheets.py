from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Импортируем конфиг
from vkusvill.config import (
    json_keyfile,
    csv_files
)

# Импортируем функции из utils
from utils_vkusvill.sheets_task import (
    upload_csv_files_to_multiple_sheets
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)   # Этот параметр ставим, в случае, если выпадет ошибка по какой-либо функции, то DAG перезапустится через 5 минут.
}

with DAG(
'vkusvill_DAG5_load_to_sheets',
default_args=default_args,
description='Загружаем полученные файлы с расчетами в google sheets',
schedule_interval=None,
start_date=days_ago(1),
catchup=False,
) as dag:

    # Загружаем полученные файлы в гугл таблицу
    upload_csv_files_to_multiple_sheets_task = PythonOperator(
        task_id= 'upload_csv_files_to_multiple_sheets',
        python_callable= upload_csv_files_to_multiple_sheets,
        op_kwargs={
            'csv_files_with_sheets': csv_files,
            'json_keyfile_path': json_keyfile
        }
    )

    upload_csv_files_to_multiple_sheets_task
