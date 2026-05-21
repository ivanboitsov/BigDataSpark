-- 1.1 Топ-10 самых продаваемых продуктов
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.rating AS avg_rating,
    p.reviews AS total_reviews,
    SUM(f.sale_quantity) AS total_quantity,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.rating,
    p.reviews
ORDER BY total_quantity DESC
LIMIT 10;

-- 1.2 Общая выручка по категориям продуктов
SELECT
    p.category,
    SUM(f.sale_quantity) AS total_quantity,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

-- 1.3 Средний рейтинг и количество отзывов для каждого продукта
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.rating AS avg_rating,
    p.reviews AS total_reviews
FROM dim_product p
ORDER BY avg_rating DESC, total_reviews DESC;

-- 2.1 Топ-10 клиентов с наибольшей общей суммой покупок
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(f.sale_total_price) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY total_spent DESC
LIMIT 10;

-- 2.2 Распределение клиентов по странам
SELECT
    l.country,
    COUNT(DISTINCT c.customer_id) AS customer_count
FROM dim_customer c
JOIN dim_location l ON c.location_id = l.location_id
GROUP BY l.country
ORDER BY customer_count DESC;

-- 2.3 Средний чек для каждого клиента
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    AVG(f.sale_total_price) as avg_check
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY avg_check DESC;

-- 3.1 Месячные и годовые тренды продаж
SELECT
    EXTRACT(YEAR FROM f.sale_date) AS year,
    EXTRACT(MONTH FROM f.sale_date) AS month,
    SUM(f.sale_quantity) AS total_quantity,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
GROUP BY year, month
ORDER BY year, month;

-- 3.2 Сравнение выручки за разные периоды (по кварталам)
SELECT
    EXTRACT(YEAR FROM f.sale_date) AS year,
    EXTRACT(QUARTER FROM f.sale_date) AS quarter,
    SUM(f.sale_quantity) AS total_quantity,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
GROUP BY year, quarter
ORDER BY year, quarter;

-- 3.3 Средний размер заказа по месяцам
SELECT
    EXTRACT(YEAR FROM f.sale_date) AS year,
    EXTRACT(MONTH FROM f.sale_date) AS month,
    AVG(f.sale_total_price) AS avg_order_size
FROM fact_sales f
GROUP BY year, month
ORDER BY year, month;

-- 4.1 Топ-5 магазинов с наибольшей выручкой
SELECT
    s.store_id,
    s.store_name,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY
    s.store_id,
    s.store_name
ORDER BY total_revenue DESC
LIMIT 5;

-- 4.2 Распределение продаж по городам и странам
SELECT
    l.country,
    l.city,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_location l ON s.location_id = l.location_id
GROUP BY
    l.country,
    l.city
ORDER BY total_revenue DESC;

-- 4.3 Средний чек для каждого магазина
SELECT
    s.store_id,
    s.store_name,
    AVG(f.sale_total_price) AS avg_check
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY
    s.store_id,
    s.store_name
ORDER BY avg_check DESC;

-- 5.1 Топ-5 поставщиков с наибольшей выручкой
SELECT
    sp.supplier_id,
    sp.supplier_name,
    SUM(f.sale_total_price) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_supplier sp ON p.supplier_id = sp.supplier_id
GROUP BY
    sp.supplier_id,
    sp.supplier_name
ORDER BY total_revenue DESC
LIMIT 5;

-- 5.2 Средняя цена товаров от каждого поставщика
SELECT
    sp.supplier_id,
    sp.supplier_name,
    AVG(p.price) AS avg_price
FROM dim_product p
JOIN dim_supplier sp ON p.supplier_id = sp.supplier_id
GROUP BY
    sp.supplier_id,
    sp.supplier_name
ORDER BY avg_price DESC;

-- 5.3 Распределение продаж по странам поставщиков
SELECT
    l.country,
    SUM(f.sale_total_price) AS total_revenue,
    SUM(f.sale_quantity) AS total_quantity
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_supplier sp ON p.supplier_id = sp.supplier_id
JOIN dim_location l ON sp.location_id = l.location_id
GROUP BY l.country
ORDER BY total_revenue DESC;

-- 6.1 Продукты с наивысшим и наименьшим рейтингом
(SELECT product_id, product_name, category, price, rating, 'top' AS rank_type
FROM dim_product ORDER BY rating DESC LIMIT 10)
UNION ALL
(SELECT product_id, product_name, category, price, rating, 'bottom' AS rank_type
FROM dim_product ORDER BY rating ASC LIMIT 10);

-- 6.2 Корреляция между рейтингом и объемом продаж
SELECT
    p.product_id,
    p.product_name,
    p.rating,
    SUM(f.sale_quantity) AS total_quantity
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.rating
ORDER BY p.rating DESC, total_quantity DESC;

-- 6.3 Продукты с наибольшим количеством отзывов
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.reviews AS total_reviews
FROM dim_product p
ORDER BY total_reviews DESC;
