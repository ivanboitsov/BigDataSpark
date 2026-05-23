# BigDataSpark
Анализ больших данных - лабораторная работа №2 - ETL-пайплайн на Apache Spark

## Содержание
1. Термины
2. Описание решения
3. Запуск
4. Результаты

## Термины
**ETL** — Extract, Transform, Load. Процесс извлечения данных из источника, их преобразования и загрузки в целевое хранилище.

**Apache Spark** — фреймворк для распределённой обработки больших данных. Позволяет выполнять трансформации над данными параллельно, в памяти.

**PySpark** — Python API для Apache Spark.

**DataFrame** — основная абстракция Spark. Таблица с именованными колонками, над которой можно выполнять SQL-подобные операции.

**JDBC** — Java Database Connectivity. Стандартный интерфейс для подключения Java-приложений (и Spark) к реляционным базам данных.

**Витрина данных (Data Mart)** — заранее агрегированный срез данных, заточенный под конкретную аналитическую задачу. В отличие от схемы снежинка, данные уже посчитаны и хранятся в плоском виде.

**ClickHouse** — колоночная СУБД, оптимизированная для аналитических запросов. Используется как целевое хранилище для витрин данных.

**MergeTree** — основной движок таблиц в ClickHouse. Требует указания `ORDER BY` для определения ключа сортировки.

**Docker** - платформа для запуска приложений в изолированных контейнерах. Контейнер содержит всё необходимое для работы приложения: код, зависимости, конфигурацию. Позволяет запускать одинаковое окружение на любой машине без ручной установки сервисов.

**Docker Compose** — инструмент для запуска многоконтейнерных приложений. Описывает все сервисы, сети и volumes в одном `docker-compose.yml`.

## Описание решения
Лабораторная работа реализует ETL-пайплайн в два этапа:

### Этап 1 — mock_data -> схема снежинка (PostgreSQL)
Spark читает сырую таблицу `mock_data` (10 000 строк) из PostgreSQL и раскладывает данные по таблицам схемы снежинка: `dim_location`, `dim_pet`, `dim_customer`, `dim_seller`, `dim_supplier`, `dim_store`, `dim_product`, `fact_sales`. Реализовано в [etl_postgres.py](/etl_postgres.py).

### Этап 2 — схема снежинка -> витрины (ClickHouse)
Spark читает таблицы схемы снежинка, выполняет агрегации и записывает результат в 18 таблиц-витрин в ClickHouse. Реализовано в [etl_clickhouse.py](/etl_clickhouse.py).

Витрины сгруппированы по 6 темам, по 3 таблицы в каждой:
1. Продажи по продуктам
2. Продажи по клиентам
3. Продажи по времени
4. Продажи по магазинам
5. Продажи по поставщикам
6. Качество продукции

Реализован следующий функционал:
1. Инициализация сырых данных — [init_mock_data.sql](/init_mock_data.sql)
2. Создание таблиц схемы снежинка — [DDL](/init_snowflake_tables.sql)
3. ETL: mock_data → схема снежинка — [etl_postgres.py](/etl_postgres.py)
4. ETL: схема снежинка → витрины ClickHouse — [etl_clickhouse.py](/etl_clickhouse.py)

## Запуск
Для запуска необходим установленный Docker.

### Ручной запуск
```bash
docker compose build
docker compose up -d
```

Для проверки PostgreSQL:
```bash
# Наличие таблиц
docker exec petshop_postgres psql -U postgres -d petshop_db -c "\dt"

# Количество строк в каждой таблице
docker exec petshop_postgres psql -U postgres -d petshop_db -c "
SELECT 'mock_data' AS tbl, COUNT(*) FROM mock_data
UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL SELECT 'dim_pet', COUNT(*) FROM dim_pet
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"
```

Для проверки ClickHouse:
```bash
docker exec petshop_clickhouse clickhouse-client \
    --user default --password your_password \
    --database reports \
    --query "SHOW TABLES"
```

### Автоматический запуск и проверка
```bash
chmod +x run_solution.sh
./run_solution.sh
```

Скрипт автоматически:
- поднимает PostgreSQL и ClickHouse
- ждёт готовности обоих сервисов
- запускает `etl_postgres.py` — заполняет схему снежинка
- запускает `etl_clickhouse.py` — формирует витрины в ClickHouse
- выводит количество строк во всех таблицах и примеры данных

## Результаты

> Таблицы схемы снежинка в PostgreSQL:

Schema |     Name     | Type  |  Owner   
-------|--------------|-------|----------
public | dim_customer | table | postgres
public | dim_location | table | postgres
public | dim_pet      | table | postgres
public | dim_product  | table | postgres
public | dim_seller   | table | postgres
public | dim_store    | table | postgres
public | dim_supplier | table | postgres
public | fact_sales   | table | postgres
public | mock_data    | table | postgres

> Количество строк в каждой таблице:

tbl          | count 
-------------|-------
mock_data    | 10000
dim_location | 27050
dim_pet      |  9321
dim_customer |  1000
dim_seller   |  1000
dim_supplier |   383
dim_store    |   383
dim_product  |  1000
fact_sales   | 10000

> Витрины и их объём в ClickHouse:

```
report_1_1_top_products	10	1.06 KiB
report_1_2_product_category_revenue	3	436.00 B
report_1_3_avg_products_rating	1000	11.56 KiB
report_2_1_top_customers	10	733.00 B
report_2_2_customer_by_country	46	802.00 B
report_2_3_customers_avg_check	1000	23.25 KiB
report_3_1_monthly_trends	12	674.00 B
report_3_2_seasonal_revenue	4	554.00 B
report_3_3_avg_order_by_month	12	509.00 B
report_4_1_top_stores	5	464.00 B
report_4_2_sales_by_city	57	1.56 KiB
report_4_3_store_avg_check	383	6.49 KiB
report_5_1_top_suppliers	5	485.00 B
report_5_2_supplier_avg_price	355	6.02 KiB
report_5_3_supplier_sales_by_country	80	1.83 KiB
report_6_1_product_rating_extremes	20	1.70 KiB
report_6_2_rating_sales_correlation	1000	8.87 KiB
report_6_3_most_reviewed_products	1000	14.92 KiB
```

> Первые строки каждой витриы в Clickhouse:

```
--- report_1_1_top_products ---
   ┌─product_id─┬─product_name─┬─category─┬─avg_rating─┬─total_reviews─┬─total_quantity─┬─total_revenue─┐
1. │        963 │ Cat Toy      │ Food     │        4.8 │           650 │             84 │       2116.63 │
2. │        562 │ Cat Toy      │ Cage     │        1.1 │           406 │             84 │       2791.52 │
3. │        692 │ Dog Food     │ Food     │        4.6 │           542 │             80 │       2964.14 │
   └────────────┴──────────────┴──────────┴────────────┴───────────────┴────────────────┴───────────────┘

--- report_1_2_product_category_revenue ---
   ┌─category─┬─total_quantity─┬─total_revenue─┐
1. │ Toy      │          18577 │     843596.01 │
2. │ Cage     │          18142 │     843146.44 │
3. │ Food     │          17904 │     843109.67 │
   └──────────┴────────────────┴───────────────┘

--- report_1_3_avg_products_rating ---
   ┌─product_id─┬─product_name─┬─category─┬─avg_rating─┬─total_reviews─┐
1. │         17 │ Bird Cage    │ Food     │          5 │           882 │
2. │        851 │ Bird Cage    │ Toy      │          5 │           537 │
3. │        622 │ Cat Toy      │ Toy      │          5 │           512 │
   └────────────┴──────────────┴──────────┴────────────┴───────────────┘

--- report_2_1_top_customers ---
   ┌─customer_id─┬─first_name─┬─last_name─┬─total_spent─┐
1. │         611 │ Mile       │ Tuer      │     4005.98 │
2. │         779 │ Lewiss     │ Pinshon   │     3784.44 │
3. │         434 │ Alexis     │ Quinton   │     3751.09 │
   └─────────────┴────────────┴───────────┴─────────────┘

--- report_2_2_customer_by_country ---
   ┌─country─────┬─customer_count─┐
1. │ Russia      │             47 │
2. │ Philippines │             46 │
3. │ Poland      │             46 │
   └─────────────┴────────────────┘

--- report_2_3_customers_avg_check ---
   ┌─customer_id─┬─first_name─┬─last_name─┬─avg_check─┐
1. │         611 │ Mile       │ Tuer      │   400.598 │
2. │         779 │ Lewiss     │ Pinshon   │   378.444 │
3. │         434 │ Alexis     │ Quinton   │   375.109 │
   └─────────────┴────────────┴───────────┴───────────┘

--- report_3_1_monthly_trends ---
   ┌─year─┬─month─┬─total_quantity─┬─total_revenue─┐
1. │ 2021 │     1 │           4856 │     224158.54 │
2. │ 2021 │     2 │           4070 │     192348.31 │
3. │ 2021 │     3 │           4561 │      207282.2 │
   └──────┴───────┴────────────────┴───────────────┘

--- report_3_2_seasonal_revenue ---
   ┌─year─┬─quarter─┬─total_quantity─┬─total_revenue─┐
1. │ 2021 │       1 │          13487 │     623789.05 │
2. │ 2021 │       2 │          13453 │     633400.48 │
3. │ 2021 │       3 │          14075 │     652395.72 │
   └──────┴─────────┴────────────────┴───────────────┘

--- report_3_3_avg_order_by_month ---
   ┌─year─┬─month─┬─avg_order_size─┐
1. │ 2021 │     1 │     256.474302 │
2. │ 2021 │     2 │     260.281881 │
3. │ 2021 │     3 │     245.886358 │
   └──────┴───────┴────────────────┘

--- report_4_1_top_stores ---
   ┌─store_id─┬─store_name─┬─total_revenue─┐
1. │      197 │ Mynte      │      15751.71 │
2. │      229 │ Quatz      │      15176.64 │
3. │      145 │ Jayo       │      13976.01 │
   └──────────┴────────────┴───────────────┘

--- report_4_2_sales_by_city ---
   ┌─country─┬─city────────┬─total_revenue─┐
1. │ France  │ Reims       │      15176.64 │
2. │ Peru    │ Cortinhas   │      12731.63 │
3. │ China   │ Saint-Denis │      11235.31 │
   └─────────┴─────────────┴───────────────┘

--- report_4_3_store_avg_check ---
   ┌─store_id─┬─store_name─┬──avg_check─┐
1. │       31 │ Brightbean │ 335.689545 │
2. │       82 │ Eamia      │ 319.268621 │
3. │      365 │ Youopia    │ 316.746875 │
   └──────────┴────────────┴────────────┘

--- report_5_1_top_suppliers ---
   ┌─supplier_id─┬─supplier_name─┬─total_revenue─┐
1. │         344 │ Wikizz        │      25740.26 │
2. │         306 │ Topicshots    │      20597.66 │
3. │         301 │ Thoughtstorm  │      20560.43 │
   └─────────────┴───────────────┴───────────────┘

--- report_5_2_supplier_avg_price ---
   ┌─supplier_id─┬─supplier_name─┬─avg_price─┐
1. │         338 │ Voonix        │     95.25 │
2. │          44 │ Buzzshare     │     94.25 │
3. │         119 │ Gabtune       │     93.01 │
   └─────────────┴───────────────┴───────────┘

--- report_5_3_supplier_sales_by_country ---
   ┌─country───┬─total_revenue─┬─total_quantity─┐
1. │ China     │     517739.58 │          11212 │
2. │ Indonesia │     314239.68 │           6828 │
3. │ Russia    │     173381.53 │           3542 │
   └───────────┴───────────────┴────────────────┘

--- report_6_1_product_rating_extremes ---
   ┌─product_id─┬─product_name─┬─category─┬─price─┬─rating─┬─rank_type─┐
1. │        323 │ Dog Food     │ Cage     │  3.25 │      1 │ bottom    │
2. │        578 │ Cat Toy      │ Toy      │ 69.95 │      1 │ bottom    │
3. │        334 │ Cat Toy      │ Food     │ 91.38 │      1 │ bottom    │
   └────────────┴──────────────┴──────────┴───────┴────────┴───────────┘

--- report_6_2_rating_sales_correlation ---
   ┌─product_id─┬─product_name─┬─rating─┬─total_quantity─┐
1. │        622 │ Cat Toy      │      5 │             77 │
2. │        851 │ Bird Cage    │      5 │             69 │
3. │        182 │ Dog Food     │      5 │             59 │
   └────────────┴──────────────┴────────┴────────────────┘

--- report_6_3_most_reviewed_products ---
   ┌─product_id─┬─product_name─┬─category─┬─price─┬─total_reviews─┐
1. │        732 │ Bird Cage    │ Food     │ 55.84 │          1000 │
2. │        874 │ Dog Food     │ Food     │  44.9 │          1000 │
3. │        464 │ Bird Cage    │ Food     │    77 │           999 │
   └────────────┴──────────────┴──────────┴───────┴───────────────┘
```
