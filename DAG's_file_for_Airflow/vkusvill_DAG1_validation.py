from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Импортируем конфиг
from vkusvill.config import (
    input_file_events,
    input_file_orders
)

# Импортируем таски
from utils_vkusvill.validation_task import (
    check_file_exists,
    validate_columns_events,
    validate_columns_orders,
    validate_order_event_relation,
    validate_order_product_pairs
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)   # Этот параметр ставим, в случае, если выпадет ошибка по какой-либо функции, то DAG перезапустится через 5 минут.
}

with DAG(
'vkusvill_DAG1_data_validation',
default_args=default_args,
description='Валидация входных файлов events и orders',
schedule_interval='0 0 * * *', # запуск каждый день в 00:00
start_date=days_ago(1),
catchup=False,
) as dag:

    # 1. Проверка файлов
    check_file_exists_task = PythonOperator(
        task_id = 'check_file_exists',
        python_callable = check_file_exists,
        op_kwargs = {
            'events_path': input_file_events,
            'orders_path' : input_file_orders
        }
    )

    # 2. Проверка колонок events
    validate_columns_events_task = PythonOperator(
        task_id = 'validate_columns_events',
        python_callable = validate_columns_events,
        op_kwargs = {'file_path': input_file_events}
    )

    # 3. Проверка колонок orders
    validate_columns_orders_task = PythonOperator(
        task_id = 'validate_columns_orders',
        python_callable = validate_columns_orders,
        op_kwargs = {'file_path': input_file_orders}
    )

    # 4. Проверка соответствия строк в events.csv
    validate_order_event_relation_task = PythonOperator(
        task_id = 'validate_order_event_relation',
        python_callable = validate_order_event_relation,
        op_kwargs = {'events_path': input_file_events}
    )

    # 5. Проверка пар order_id + product_id в файлах events.csv и orders.csv
    validate_order_product_pairs_task = PythonOperator(
        task_id = 'validate_order_product_pairs',
        python_callable = validate_order_product_pairs,
        op_kwargs = {
            'events_path': input_file_events,
            'orders_path': input_file_orders
        }
    )

    # Триггер запуска следующего DAG
    trigger_preprocessing = TriggerDagRunOperator(
        task_id = 'trigger_next_dag_preprocessing',
        trigger_dag_id = 'vkusvill_DAG2_data_preprocessing'
    )

    check_file_exists_task >> validate_columns_events_task >> validate_columns_orders_task \
    >> validate_order_event_relation_task >> validate_order_product_pairs_task \
    >> trigger_preprocessing