from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import sqlite3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

default_args = {
    'owner': 'airflow',
    'depends_on_past' : False,
    'retries': 1,
}

def check_file_exists(events_path, orders_path):
    """
    Проверка наличия файлов.
    Функция принимает путь к файлам и проверяет, существуют ли файлы по указанному пути.
    Если файлы не найдены, выбрасывает исключение FileNotFoundError.

    Аргументы:
        events_path (str): Путь к проверяемому файлу 'events.csv'.
        orders_path (str): Путь к проверяемому файлу 'orders.csv'.

    Исключения:
        FileNotFoundError: Выбрасывается, если файл отсутствует по указанному пути.
    """

    # Проверяем файл 'events.csv'
    if not os.path.exists(events_path):
        raise FileNotFoundError(f"Основной файл '{events_path}' не найден!")

    # Проверяем файл 'orders'csv'
    if not os.path.exists(orders_path):
        raise FileNotFoundError(f"Основной файл '{orders_path}' не найден!")

    print('Все файлы найдены')



def validate_columns_events(file_path):
    """
    Проверка наличия обязательных столбцов в файле.

    Функция загружает CSV-файл, проверяет наличие необходимых столбцов
    и выбрасывает исключение, если каких-либо столбцов не хватает.

    Аргумент:
        file_path (str): Пусть к файлу, который необходимо проверить.

    Исключения:
        ValueError: Выбрасывается, если отсутствует обязательные столбцы.

    Логика работы:
    1. Читаем файл с помощью pandas.
    2. Определяем список обязательных столбцов.
    3. Сравниваем обязательные столбцы с реальными столбцами из файла
    4. Если каких-то столбцов не хватает, выбрасываем исключение с их перечнем.
    """

    # Чтение CSV-файла
    df = pd.read_csv(file_path)

    # Список обязательных столбцов
    required_columns = ['datetime', 'user_id', 'event_name', 'page', 'product_id', 'order_id']

    # Поиск отсутствующих столбцов
    missing_columns = [col for col in required_columns if col not in df.columns]

    # Если отсутсвуют обязательные столбцы, выбрасываем исключение
    if missing_columns:
        raise ValueError(f'Не хватает столбцов: {', '.join(missing_columns)}')

    print(f'Файл "{file_path}" корректен.')


def validate_columns_orders(file_path):
    """
    Проверка наличия обязательных столбцов в файле.

    Функция загружает CSV-файл, проверяет наличие необходимых столбцов
    и выбрасывает исключение, если каких-либо столбцов не хватает.

    Аргумент:
        file_path (str): Пусть к файлу, который необходимо проверить.

    Исключения:
        ValueError: Выбрасывается, если отсутствует обязательные столбцы.

    Логика работы:
    1. Читаем файл с помощью pandas.
    2. Определяем список обязательных столбцов.
    3. Сравниваем обязательные столбцы с реальными столбцами из файла
    4. Если каких-то столбцов не хватает, выбрасываем исключение с их перечнем.
    """

    # Чтение CSV-файла
    df = pd.read_csv(file_path)

    # Список обязательных столбцов
    required_columns = ['datetime', 'user_id', 'product_id', 'order_id', 'revenue']

    # Поиск отсутствующих столбцов
    missing_columns = [col for col in required_columns if col not in df.columns]

    # Если отсутсвуют обязательные столбцы, выбрасываем исключение
    if missing_columns:
        raise ValueError(f'Не хватает столбцов: {', '.join(missing_columns)}')

    print(f'Файл "{file_path}" корректен.')


def validate_order_event_relation(events_path):
    """
    Проверяет файл 'events.csv', что все строки, где order_id не пустой,
    имеют event_name == 'purchase'.
    Если находит ошибки — вызывает Exception.

    Аргументы:
        events_path (str): Путь к файлу events_new.csv

    Исключения:
        Exception: Выбрасывается, если не пустые значения в столбце order_id равны не только значению 'purchase'
                   в столбце event_name. 
    """

    df = pd.read_csv(events_path)

    # Строки, в которых есть order_id, но event_name не 'purchase'
    invalid_rows = df[
        (df['order_id'].notna()) &
        (df['order_id'] != '') &
        (df['event_name'] != 'purchase')
    ]

    if not invalid_rows.empty:
        # Формируем удобочитаемое сообщение
        message = (
            "Найдены строки, где order_id указан, но event_name != 'purchase'.\n"
            f"Количество проблемных строк: {invalid_rows.shape[0]}\n"
            f"Примеры:\n{invalid_rows.head().to_string(index=False)}"
        )
        raise Exception(message)

    print('Все значения в столбце order_id соответсвуют значению "purchase" в столбце event_name')


def validate_order_product_pairs(events_path, orders_path):
    """
    Проверяет, что все пары (order_id, product_id) из orders.csv
    присутствуют среди непустых пар в events.csv.
    Если находятся расхождения — вызывает Exception.

    Аргументы:
        events_path: Путь к файлу events.csv.
        orders_path: Путь к файлу events.csv.

    Исключения:
        Execption: Выбрасывается, если пары order_id и product_id не совпадают в таблицах events_new.csv и orders.csv.

    Логика работы:
        1) Загружаем таблицы.
        2) Берем НЕ пустые строки в events_new из столбцов order_id и product_id.
        3) Создаем множество пар.
        4) Ищем пары, которых нет в events_new.
        5) Делаем исключение, если есть несоответствие.
    """

    # Загружаем таблицы
    events_df = pd.read_csv(events_path)
    orders_df = pd.read_csv(orders_path)

    # Берём только строки из events_new, где order_id и product_id НЕ пустые
    events_filtered = events_df[
        (events_df['order_id'].notna()) &
        (events_df['order_id'] != '') &
        (events_df['product_id'].notna()) &
        (events_df['product_id'] != '')
    ]

    # Создаём множества пар
    event_pairs = set(zip(events_filtered['order_id'], events_filtered['product_id']))
    order_pairs = set(zip(orders_df['order_id'], orders_df['product_id']))

    # Ищем пары, которых нет в events_new
    missing_pairs = order_pairs - event_pairs

    # Если есть несоответствия — кидаем ошибку
    if missing_pairs:
        # Преобразуем в список и покажем до 10 примеров
        missing_examples = list(missing_pairs)[:10]

        message = (
            "Обнаружены пары (order_id, product_id), которых НЕТ в events.csv.\n"
            f"Количество отсутствующих пар: {len(missing_pairs)}\n"
            f"Примеры отсутствующих пар: {missing_examples}"
        )
        raise Exception(message)

    print('Значения в столбцах order_id и product_id в файлах "orders.csv" и "events.csv" совпадают.')


def merge_events_with_orders(events_path, orders_path, output_path):
    """
    Объединяет таблицы events_new.csv и orders.csv по order_id,
    добавляя столбец revenue из orders.csv.
    Полученный результат сохраняет в новый CSV файл.

    Аргументы:
        events_path: Путь к файлу events.csv.
        orders_path: Путь к файлу orders.csv.
        output_path: Путь к новому сохраняемому файлу.
    """
    
    # Загружаем таблицы
    events_df = pd.read_csv(events_path)
    orders_df = pd.read_csv(orders_path)

    # Объединяем только по order_id
    merged_df = events_df.merge(
        orders_df[['order_id', 'revenue']], 
        on='order_id',
        how='left'   # чтобы не терять строки events, где order_id пустой
    )

    # Исправляем типы
    merged_df['order_id'] = merged_df['order_id'].astype('Int64')
    if 'product_id' in merged_df.columns:
        merged_df['product_id'] = merged_df['product_id'].astype('Int64')


    # Сохраняем результат
    merged_df.to_csv(output_path, index=False)


def check_processed_file(output_file):
    """
    Проверка преобразованного файла.

    Функция выполняет несколько проверок преобразованного файла:
    1. Проверяет, существует ли файл по указанному пути.
    2. Проверяет, что файл не пустой.
    3. Проверяет наличие обязательных столбцов, которые должны быть добавлены в ходе обработки данных.

    Аргументы:
        output_file (str): Путь к преобразованному CSV-файлу.

    Исключения:
        - FileNotFoundError, если файл не найден.
        - ValueError, если файл пуст или не содержит обязательных столбцов.
    """

    # Проверка существования файла
    if not os.path.exists(output_file):
        raise FileNotFoundError(f'Преобразованный файл {output_file} не найден.')

    # Чтение данных из преобразованного файла
    df = pd.read_csv(output_file)

    # Проверка, что файл не пуст
    if df.empty:
        raise ValueError('Преобразованный файл пустой.')

    # Проверка наличия обязательных столбцов
    if ('datetime' not in df.columns 
        or 'user_id' not in df.columns 
        or 'event_name' not in df.columns 
        or 'page' not in df.columns 
        or 'product_id' not in df.columns 
        or 'order_id' not in df.columns 
        or 'revenue' not in df.columns
    ):
        raise ValueError(
            'В преобразованном файле не хватает обязательных столбцов: "datime", "user_id", "event_name",\
            "page", "product_id", "order_id" или "revenue".'
        )

    print('Файл найден и все необходимые столбцы присутствуют')


def init_db(db_path):
    """
    Инициализация базы данных SQLite.

    Функция выполняет следующие действия:
    1. Создает соединение с базой данных по указанному пути.
    2. Создает таблицу в базе данных, если она не существует.
    3. Выполняет команду для создания таблицы с определенной схемой.
    4. Закрывает соединение с базой данных после выполнения операции.

    Аргументы:
        db_path (str): Путь к базе данных SQLite, куда будет создана таблица.
    """

    # Устанавливаем соединение с базой данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)

    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    cursor.execute('''
       DROP TABLE IF EXISTS analysis_vv; 
    ''')

    # SQL-запрос для создания таблицы, если она не существует
    cursor.execute('''
        CREATE TABLE analysis_vv (
            datetime TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            event_name TEXT NOT NULL,
            page TEXT NOT NULL,
            product_id INTEGER,
            order_id INTEGER,
            revenue REAL
        );
    ''')

    # Подтвержаем изменения и записываем их в базу данных
    conn.commit()

    # Закрываем соединение с базой данных
    conn.close()


def load_data_to_db(db_path, processed_file):
    """
    Загрузка обработанных данных в таблицу базы данных SQLite.

    Функция выполняет следующие действия:
    1. Открывает соединение с базой данных.
    2. Загружает данные из обработанного файла (CSV) в таблицу.
    3. Закрывает соединение с базой данных.

    Аргументы:
        db_path (str): Путь к базе данных SQLite.
        processed_file (str): Путь к файлу с обработанными данными (CSV).
    """

    # Открываем соединение с базой данных
    conn = sqlite3.connect(db_path)

    # Читаем данные из обработанного CSV файла в DataFrame
    df = pd.read_csv(processed_file)

    # Загружаем данные в таблицу 'analysis_vv' в базе данных
    df.to_sql('analysis_vv', conn, if_exists='append', index=False)

    # Подтвержаем изменения и записываем их в базу данных
    conn.commit()

    # Закрываем соединение с базой данных
    conn.close()


def chek_table_data(db_path):
    """ Функция для выполнения SELECT запроса и проверки таблицы"""

    # Подключение к базе данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)

    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # Выполнение SELECT запроса, чтобы получить количество строк в таблице 'analysis_vv'
    cursor.execute('SELECT COUNT(*) FROM analysis_vv;')

    # Получаем результат запроса, который возвращает кортеж с числом строк в таблице
    row_count = cursor.fetchone()[0] # Получаем количесто строк в таблице

    # Проверка, если таблица пуста (нет данных)
    if row_count == 0:
        # Если таблица пуста, выбрасываем исключение с соответствующим сообщением
        raise ValueError('Таблица "analysis_vv" пуста.')

    # Выполняем дополнительный запрос для получения информации о столбцах таблицы
    cursor.execute('PRAGMA table_info(analysis_vv);')

    # Извлекаем имена всех столбцов из результатов запроса
    columns = [column[1] for column in cursor.fetchall()]   # Получаем имена столбцов

    # Список обязательных столбцов, которые должны присутствовать в таблице
    required_columns = [
        'datetime', 'user_id', 'event_name', 'page', 'product_id', 'order_id', 'revenue'
    ]

    # Проверка на отстутствие обязательных столбцов
    missing_columns = [col for col in required_columns if col not in columns]

    # Если какие-либо обязательные столбцы отсутствуют, выбрасывать исключение с их списком
    if missing_columns:
        raise ValueError(f'Отсутствуют обязательные столбцы: {', '.join(missing_columns)}')

    # Закрываем соединение с базой данных
    conn.close()


def funnel_users (db_path, output_path):
    """
    Расчитываем воронку пользователей по страницам и сохраняем в новый csv файл

    Аргументы:
        db_path: Путь к базе данных SQLite.
        output_path: Путь к новому создаваемому файлу CSV.
    """

    # Подключение к базе данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)

    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # Выполнение запроса, чтобы получить воронку по пользователям
    query = '''
        WITH base AS (
            SELECT
                DATE(datetime) AS data,
                user_id,
                page,
                MIN(CASE WHEN event_name = 'view'     THEN datetime END) AS step_view,    -- Определяем момент первого просмотра страницы
                MIN(CASE WHEN event_name = 'click'    THEN datetime END) AS step_click,   -- Определяем момент первого клика
                MIN(CASE WHEN event_name = 'add'      THEN datetime END) AS step_add,     -- Определяем момент первого добавления
                MIN(CASE WHEN event_name = 'purchase' THEN datetime END) AS step_purchase -- Определяем момент первой покупки
            FROM analysis_vv    -- Указываем таблицу, откуда будем рассчитывать воронку
            GROUP BY data, user_id, page   -- Группируем по дате, юзерам и страницам
        )
        SELECT
            data,
            page,
            COUNT(step_view)     AS Просмотры,  -- Кол-во Просмотров
            COUNT(step_click)    AS Клики,      -- Кол-во Кликов
            COUNT(step_add)      AS Добавления, -- Кол-во Добавлений
            COUNT(step_purchase) AS Покупки,    -- Кол-во Покупок
        
            -- единая конверсия в добавления
            IFNULL(
                ROUND(
                    CASE
                        WHEN COUNT(step_click) > 0 THEN   -- Если кол-во кликов больше 0
                            CAST(COUNT(step_add) AS FLOAT) / COUNT(step_click) * 100   -- То кол-во добавлений делим на кол-во кликов
                        WHEN COUNT(step_view) > 0 THEN    -- Если кол-во просмотров больше 0
                            CAST(COUNT(step_add) AS FLOAT) / COUNT(step_view) * 100  -- То кол-во добавлений делим на кол-во просмотров
                        ELSE NULL 
                    END
                , 2), 0
            ) AS "Конверсия в добавления %",
        
            -- конверсия в покупку из просмотров
            IFNULL(    -- IFNULL, чтобы заменить все значения NULL на 0
                ROUND(
                    CAST(COUNT(step_purchase) AS FLOAT) / NULLIF(COUNT(step_view), 0) * 100
                , 2),
            0) AS "Конверсия в покупку %"
        
        FROM base
        GROUP BY data, page
        ORDER BY page DESC;
    '''

    # Загружаем результат в DataFrame
    df = pd.read_sql_query(query, conn)

    # Сохраняем в CSV
    df.to_csv(output_path, index = False, encoding='utf-8')

    # Закрываем соединение
    conn.close()


def metrics_data (db_path, output_path):
    """
    Рассчитывает метрики:
        1) Кол-во заказов.
        2) Количество пользователей с заказами.
        3) GMV.
        4) Средний чек.
        5) Среднее кол-во товаров в заказе.

    Аргументы:
        db_path: Путь к базе данных SQLite.
        output_path: Путь к новому создаваемому файлу CSV.
    """

    # Подключение к базе данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)

    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # Выполнение запроса, чтобы получить воронку по пользователям
    query = '''
        WITH orders AS (  -- Делаем временную таблицу
            SELECT
                STRFTIME('%Y-%m-%d', datetime) AS data,  -- Запрашиваем дату по дням без времени
          		order_id,                                -- id заказов
                user_id,                                 -- id пользователей
                SUM(revenue) AS order_revenue,           -- Считаем общую сумму по выручке
                COUNT(product_id) AS items_in_order      -- Считаем кол-во продуктов
            FROM analysis_vv                             -- Указываем таблицу, где мы все будем считать
            WHERE order_id IS NOT NULL                   -- Условие, что поле "id заказа" не пустое
            GROUP BY data, order_id, user_id             -- Группируем по дате, id заказа, id пользователя
        )
        SELECT
        	data,
            COUNT(DISTINCT order_id) AS "Кол-во заказов",                 -- Считаем кол-во уникальных id заказов
            COUNT(DISTINCT user_id) AS "Кол-во пользователей с заказами", -- Считаем кол-во уникальный пользователей с закзами
            SUM(order_revenue) AS GMV,                                    -- Считаем сумму по выручке
            ROUND(AVG(order_revenue), 2) AS "Средний чек",                -- Находим средний чек
            ROUND(AVG(items_in_order), 2) AS "Среднее кол-во товаров в заказе"  -- Находим среднее кол-во товаров в заказе
        FROM orders                                                       -- Указываем временную таблицу
        GROUP BY data;                                                    -- Группируем по дате
    '''

    # Загружаем результат в DataFrame
    df = pd.read_sql_query(query, conn)

    # Сохраняем в CSV
    df.to_csv(output_path, index = False, encoding='utf-8')

    # Закрываем соединение
    conn.close()


def upload_csv_files_to_multiple_sheets(csv_files_with_sheets, json_keyfile_path):
    """
    Загружает CSV-файлы в отдельные Google Sheets, создавая листы по имени файла.
    Обрабатывает NaN и бесконечности, чтобы данные корректно загрузились.

    Аргументы:
        csv_files_with_sheets: Путь к CSV файлам, которые нужны для загрузки.
        json_keyfile_path: Путь к json ключу для доступа.
    """
    # Подключаемся к Google Sheets API
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    # Ищем файлы по указанному пути
    for csv_path, sheet_id in csv_files_with_sheets.items():
        if not os.path.isfile(csv_path):
            print(f"Файл не найден: {csv_path}")
            continue

        # Загружаем CSV
        df = pd.read_csv(csv_path)

        # Обрабатываем NaN и бесконечности
        df = df.replace({np.nan: "", np.inf: "", -np.inf: ""})

        # Имя листа по имени файла без расширения
        sheet_name = os.path.splitext(os.path.basename(csv_path))[0]

        # Открываем таблицу
        sheet = client.open_by_key(sheet_id)

        # Создаём или очищаем лист
        try:
            worksheet = sheet.worksheet(sheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=sheet_name, rows=str(len(df)+1), cols=str(len(df.columns))
            )

        # Загружаем данные
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Загружен CSV '{csv_path}' в таблицу '{sheet_id}', лист '{sheet_name}'")


# Определяем сам DAG
with DAG(
    'vkusvill_analysis_dag',  # Название пайплайна
    default_args = default_args,  # Стандартные аргументы
    description = 'Пайплайн для обработки файла, создания витрины и загрузки в google sheets',  # Описание
    schedule_interval = '0 0 1 * *',  # Пайплайн будет запускаться первого числа каждого месяца
    start_date = days_ago(1),  # Начальная дата запуска
    catchup = False,  # Не выполнять пропущенные запуски
) as dag:

    # Пути к необходимым файлам (датасеты, база данных SQLite, json-ключ)
    input_file_events = '/opt/airflow/dags/vkusvill/events.csv'
    input_file_orders = '/opt/airflow/dags/vkusvill/orders.csv'
    db_path = '/opt/airflow/dags/vkusvill/analysis_vv.db'
    output_file = '/opt/airflow/dags/vkusvill/processed_data.csv'
    output_file_funnel = '/opt/airflow/dags/vkusvill/funnel.csv'
    output_file_metrics = '/opt/airflow/dags/vkusvill/metrics.csv'
    json_keyfile = '/opt/airflow/dags/vkusvill/project-vkusvill-e6e3f5b46011.json'
    csv_files = {
        output_file: '1dhfnzmz77qUaNIaHfqyNYAUYfAxYakT-Uw_6kNqAJgQ',
        output_file_funnel: '1mhJrMUPOZ0mUwoO3ZrrZj6ofaaq7GJFQEHHaY4u1rF0',
        output_file_metrics: '1F7Tl6Lj1MkouH4-G3FW1HtHVQKFN3gOBUQIkBvSxlZY'
    }

    # Task 1. Проверка наличия файлов
    check_file_task = PythonOperator(
        task_id = 'check_file_exists',
        python_callable = check_file_exists,  # Функция для проверки файла
        op_args = [input_file_events, input_file_orders],  # Параметры для функции
    )

    # Task 2. Проверка наличия обязательных столбцов в файле events.csv
    validate_columns_events_task = PythonOperator(
        task_id = 'validate_columns_events',
        python_callable = validate_columns_events,
        op_args = [input_file_events]
    )

    # Task 3. Проверка наличия обязательных столбцов в файле orders.csv
    validate_columns_orders_task = PythonOperator(
        task_id = 'validate_columns_orders',
        python_callable = validate_columns_orders,
        op_args = [input_file_orders]
    )

    # Task 4. Проверяет файл 'events.csv', что все строки, где order_id не пустой имеют event_name == 'purchase'
    validate_order_event_relation_task = PythonOperator(
        task_id = 'validate_order_event_relation',
        python_callable = validate_order_event_relation,
        op_args = [input_file_events]
    )

    # Task 5. Проверяет, что все пары (order_id, product_id) из orders.csv присутствуют среди непустых пар в events.csv
    validate_order_product_pairs_task = PythonOperator(
        task_id = 'validate_order_product_pairs',
        python_callable = validate_order_product_pairs,
        op_args = [input_file_events, input_file_orders]
    )

    # Task 6. Объединяет таблицы events_new.csv и orders.csv по order_id добавляя столбец revenue из orders.csv
    merge_events_with_orders_task = PythonOperator(
        task_id = 'merge_events_with_orders',
        python_callable = merge_events_with_orders,
        op_args = [input_file_events, input_file_orders, output_file]
    )

    # Task 7. Проверка преобразованного файла
    check_processed_file_task = PythonOperator(
        task_id = 'check_processed_file',
        python_callable = check_processed_file,
        op_args = [output_file]
    )

    # Task 8. Инициализация базы данных SQLite
    init_db_task = PythonOperator(
        task_id = 'init_db',
        python_callable = init_db,
        op_args = [db_path]
    )

    # Task 9. Загрузка обработанных данных в таблицу базы данных SQLite
    load_data_to_db_task = PythonOperator(
        task_id = 'load_data_to_db',
        python_callable = load_data_to_db,
        op_args = [db_path, output_file]
    )

    # Task 10. Функция для выполнения SELECT запроса и проверки таблицы
    chek_table_data_task = PythonOperator(
        task_id = 'chek_table_data',
        python_callable = chek_table_data,
        op_args = [db_path]
    )

    # Task 11. Расчитывает воронку пользователей по страницам и сохраняет в новый CSV файл
    funnel_users_task = PythonOperator(
        task_id = 'funnel_users',
        python_callable = funnel_users,
        op_args = [db_path, output_file_funnel]
    )

    # Task 12. Рассчитывает метрики и сохраняет в новый CSV файл 
    metrics_data_task = PythonOperator(
        task_id = 'metrics_data',
        python_callable = metrics_data,
        op_args = [db_path, output_file_metrics]
    )

    # Task 13. Загружает CSV файлы с готовым анализом в Google Sheets
    upload_csv_files_to_multiple_sheets_task = PythonOperator(
        task_id = 'upload_csv_files_to_multiple_sheets',
        python_callable = upload_csv_files_to_multiple_sheets,
        op_args = [csv_files, json_keyfile]
    )

    # Пустые таски для начала и конца пайплайна
    start_task = PythonOperator(
        task_id = 'start',  # Название задачи
        python_callable = lambda: print('Пайплайн завершен'),  # Псевдофункция для завершения
        dag = dag,
    )

    end_task = PythonOperator(
        task_id = 'end',
        python_callable = lambda: print('Пайплайн завершен'), 
        dag = dag,
    )

    # Последовательность выполнения задач
    start_task >> check_file_task >> [
                                        validate_columns_events_task,
                                        validate_columns_orders_task
                                     ] \
    >> validate_order_event_relation_task >> validate_order_product_pairs_task >> merge_events_with_orders_task \
    >> check_processed_file_task >> init_db_task >> load_data_to_db_task >> chek_table_data_task \
    >> funnel_users_task >> metrics_data_task >> upload_csv_files_to_multiple_sheets_task >> end_task