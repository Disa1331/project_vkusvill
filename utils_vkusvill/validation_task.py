import os
import pandas as pd

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
    Проверка наличия обязательных столбцов в файле events.csv.

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
    Проверка наличия обязательных столбцов в файле orders.csv.

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
        events_path (str): Путь к файлу events.csv

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