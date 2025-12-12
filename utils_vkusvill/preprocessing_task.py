import pandas as pd
import os

def merge_events_with_orders(events_path, orders_path, output_path):
    """
    Объединяет таблицы events.csv и orders.csv по order_id,
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

