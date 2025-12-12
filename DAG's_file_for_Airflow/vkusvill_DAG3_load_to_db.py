from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Импортируем конфиг
from vkusvill.config import (
    db_path,
    output_file
)

# Импортируем функции из utils
from utils_vkusvill.db_task import (
    init_db,
    load_data_to_db,
    chek_table_data
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)   # Этот параметр ставим, в случае, если выпадет ошибка по какой-либо функции, то DAG перезапустится через 5 минут.
}

with DAG(
'vkusvill_DAG3_load_to_db',
default_args=default_args,
description='Создание базы данных и загрузка обработанных данных в БД SQLite',
schedule_interval=None,
start_date=days_ago(1),
catchup=False,
) as dag:

    # 1. Инициализация базы данных в SQLite
    init_db_task = PythonOperator(
        task_id= 'init_db',
        python_callable= init_db,
        op_kwargs={'db_path': db_path}
    )

    # 2. Загрузка обработанных данных в таблицу SQLite
    load_data_to_db_task = PythonOperator(
        task_id= 'load_data_to_db',
        python_callable= load_data_to_db,
        op_kwargs={
            'db_path': db_path,
            'processed_file': output_file
        }
    )

    # 3. Проверка таблицы
    chek_table_data_task = PythonOperator(
        task_id= 'chek_table_data',
        python_callable= chek_table_data,
        op_kwargs={'db_path': db_path}
    )

    # Триггер следующего DAG
    trigger_analytics = TriggerDagRunOperator(
        task_id = 'trigger_next_dag_analytics',
        trigger_dag_id = 'vkusvill_DAG4_analytics'
    )

    init_db_task >> load_data_to_db_task >> chek_table_data_task >> trigger_analytics