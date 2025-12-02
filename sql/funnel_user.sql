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
