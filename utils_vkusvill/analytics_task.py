import pandas as pd
import sqlite3
import os

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
                            CAST(COUNT(step_add) AS FLOAT) / COUNT(step_click) * 100   -- То количество добавлений делим на кол-во кликов
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
    df.to_csv(output_path, index = False, encoding='utf-8-sig') # указываем такую кодировку, чтобы данные корректно отображались в Excel

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
    df.to_csv(output_path, index = False, encoding='utf-8-sig')

    # Закрываем соединение
    conn.close()

