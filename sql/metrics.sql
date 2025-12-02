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
GROUP BY data;
