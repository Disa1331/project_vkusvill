from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Импортируем конфиг
from vkusvill.config import (
    input_file_events,
    input_file_orders,
    output_file
)

# Импортируем функции из utils
from utils_vkusvill.preprocessing_task import (
    merge_events_with_orders,
    check_processed_file
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)   # Этот параметр ставим, в случае, если выпадет ошибка по какой-либо функции, то DAG перезапустится через 5 минут.
}

with DAG(
'vkusvill_DAG2_data_preprocessing',
default_args=default_args,
description='Объединение таблиц events.csv и orders.csv; Сохранение в новый файл processed_data.csv; Проверка преобразованного файла',
schedule_interval=None,
start_date=days_ago(1),
catchup=False,
) as dag:

    # 1. Объединяем таблицы
    merge_events_with_orders_task = PythonOperator(
        task_id= 'merge_events_with_orders',
        python_callable= merge_events_with_orders,
        op_kwargs={
            'events_path': input_file_events,
            'orders_path': input_file_orders,
            'output_path': output_file
        }
    )

    # 2. Проверка преобразованного файла
    check_processed_file_task = PythonOperator(
        task_id= 'check_processed_file',
        python_callable= check_processed_file,
        op_kwargs= {'output_file': output_file}
    )

    # Триггер следующего DAG
    trigger_load_to_db = TriggerDagRunOperator(
        task_id = 'trigger_next_dag_db',
        trigger_dag_id = 'vkusvill_DAG3_load_to_db'
    )


    merge_events_with_orders_task >> check_processed_file_task >> trigger_load_to_db
