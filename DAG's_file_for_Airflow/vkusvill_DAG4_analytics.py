from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Импортируем конфиг
from vkusvill.config import (
    db_path,
    output_file_funnel,
    output_file_metrics
)

# Импортируем функции из utils
from utils_vkusvill.analytics_task import (
    funnel_users,
    metrics_data
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)   # Этот параметр ставим, в случае, если выпадет ошибка по какой-либо функции, то DAG перезапустится через 5 минут.
}

with DAG(
'vkusvill_DAG4_analytics',
default_args=default_args,
description='Сздаем витрину данных по воронке и метрикам. Сохраняем в два новых файла CSV',
schedule_interval=None,
start_date=days_ago(1),
catchup=False,
) as dag:

    # 1. Расчитываем воронку пользователей и сохраняем в файл funnel.csv
    funnel_users_task = PythonOperator(
        task_id= 'funnel_users',
        python_callable= funnel_users,
        op_kwargs={
            'db_path': db_path,
            'output_path': output_file_funnel
        }
    )

    # 2. Рассчитываем метрики и сохраняем в файл metrics.csv
    metrics_data_task = PythonOperator(
        task_id= 'metrics_data',
        python_callable= metrics_data,
        op_kwargs={
            'db_path': db_path,
            'output_path': output_file_metrics
        }
    )

    # Триггер следующего DAG
    trigger_sheets = TriggerDagRunOperator(
        task_id = 'trigger_next_load_to_sheets',
        trigger_dag_id = 'vkusvill_DAG5_load_to_sheets'
    )

    funnel_users_task >> metrics_data_task >> trigger_sheets