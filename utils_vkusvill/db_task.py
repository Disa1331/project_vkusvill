import sqlite3
import pandas as pd
import os

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
