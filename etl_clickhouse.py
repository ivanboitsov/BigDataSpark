from pyspark.sql import SparkSession
from pyspark.sql import functions

from core.config import PG_URL, PG_PROPS, PG_JAR, CH_URL, CH_PROPS, CH_JAR

def write_to_clickhouse(df, table_name):
    print(f"Writing {table_name}...")
    df.write.jdbc(
        url=CH_URL,
        table=table_name,
        mode="overwrite",
        properties=CH_PROPS,
    )


spark = SparkSession.builder \
    .appName("ETL: PostgresSQL -> ClickHouse") \
    .config("spark.jars", f"{PG_JAR},{CH_JAR}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading star schema tables...")
dim_location = spark.read.jdbc(url=PG_URL, table="dim_location", properties=PG_PROPS)
dim_pet = spark.read.jdbc(url=PG_URL, table="dim_pet", properties=PG_PROPS)
dim_customer = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPS)
dim_seller = spark.read.jdbc(url=PG_URL, table="dim_seller", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(url=PG_URL, table="dim_supplier", properties=PG_PROPS)
dim_store = spark.read.jdbc(url=PG_URL, table="dim_store", properties=PG_PROPS)
dim_product = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPS)
fact_sales = spark.read.jdbc(url=PG_URL, table="fact_sales", properties=PG_PROPS)


# 1.1 Топ-10 самых продаваемых продуктов
showcase_1_1 = fact_sales \
    .join(dim_product, "product_id") \
    .groupBy(
        "product_id",
        "product_name",
        "category",
        functions.col("rating").alias("avg_rating"),
        functions.col("reviews").alias("total_reviews")
    ) \
    .agg(
        functions.sum("sale_quantity").alias("total_quantity"),
        functions.sum("sale_total_price").alias("total_revenue"),
    ) \
    .orderBy(functions.col("total_quantity").desc()).limit(10)

# 1.2 Общая выручка по категориям продуктов
showcase_1_2 = fact_sales \
    .join(dim_product, "product_id") \
    .groupBy("category") \
    .agg(
        functions.sum("sale_quantity").alias("total_quantity"),
        functions.sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy(functions.col("total_revenue").desc())

# 1.3 Средний рейтинг и количество отзывов для каждого продукта
showcase_1_3 = dim_product \
    .select(
        "product_id",
        "product_name",
        "category",
        functions.col("rating").alias("avg_rating"),
        functions.col("reviews").alias("total_reviews")
    ).orderBy(functions.col("avg_rating").desc(), functions.col("total_reviews").desc())

# 2.1 Топ-10 клиентов с наибольшей общей суммой покупок
showcase_2_1 = fact_sales \
    .join(dim_customer, "customer_id") \
    .groupBy("customer_id", "first_name", "last_name") \
    .agg(functions.sum("sale_total_price").alias("total_spent")) \
    .orderBy(functions.col("total_spent").desc()) \
    .limit(10)

# 2.2 Распределение клиентов по странам
showcase_2_2 = dim_customer \
    .join(dim_location, "location_id") \
    .groupBy("country") \
    .agg(functions.countDistinct("customer_id").alias("customer_count")) \
    .orderBy(functions.col("customer_count").desc())

# 2.3 Средний чек для каждого клиента
showcase_2_3 = fact_sales \
    .join(dim_customer, "customer_id") \
    .groupBy("customer_id", "first_name", "last_name") \
    .agg(functions.avg("sale_total_price").alias("avg_check")) \
    .orderBy(functions.col("avg_check").desc())

# 3.1 Месячные и годовые тренды продаж
showcase_3_1 = fact_sales \
    .groupBy(
        functions.year("sale_date").alias("year"),
        functions.month("sale_date").alias("month")
    ) \
    .agg(
        functions.sum("sale_quantity").alias("total_quantity"),
        functions.sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy("year", "month")

# 3.2 Сравнение выручки за разные периоды
showcase_3_2 = fact_sales \
    .groupBy(
        functions.year("sale_date").alias("year"),
        functions.quarter("sale_date").alias("quarter")
    ) \
    .agg(
        functions.sum("sale_quantity").alias("total_quantity"),
        functions.sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy("year", "quarter")

# 3.3 Средний размер заказа по месяцам
showcase_3_3 = fact_sales \
    .groupBy(
        functions.year("sale_date").alias("year"),
        functions.month("sale_date").alias("month")
    ) \
    .agg(functions.avg("sale_total_price").alias("avg_order_size")) \
    .orderBy("year", "month")

# 4.1 Топ-5 магазинов с наибольшей выручкой
showcase_4_1 = fact_sales \
    .join(dim_store, "store_id") \
    .groupBy("store_id", "store_name") \
    .agg(functions.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(functions.col("total_revenue").desc()) \
    .limit(5)

# 4.2 Распределение продаж по городам и странам
showcase_4_2 = fact_sales \
    .join(dim_store, "store_id") \
    .join(dim_location, dim_store.location_id == dim_location.location_id) \
    .groupBy("country", "city") \
    .agg(functions.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(functions.col("total_revenue").desc())

# 4.3 Средний чек для каждого магазина
showcase_4_3 = fact_sales \
    .join(dim_store, "store_id") \
    .groupBy("store_id", "store_name") \
    .agg(functions.avg("sale_total_price").alias("avg_check")) \
    .orderBy(functions.col("avg_check").desc())

# 5.1 Топ-5 поставщиков с наибольшей выручкой
showcase_5_1 = fact_sales \
    .join(dim_product, "product_id") \
    .join(dim_supplier, "supplier_id") \
    .groupBy("supplier_id", "supplier_name") \
    .agg(functions.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(functions.col("total_revenue").desc()).limit(5)

# 5.2 Средняя цена товаров от каждого поставщика
showcase_5_2 = dim_product \
    .join(dim_supplier, "supplier_id") \
    .groupBy("supplier_id", "supplier_name") \
    .agg(functions.avg("price").alias("avg_price")) \
    .orderBy(functions.col("avg_price").desc())

# 5.3 Распределение продаж по странам поставщиков
showcase_5_3 = fact_sales \
    .join(dim_product, "product_id") \
    .join(dim_supplier, "supplier_id") \
    .join(dim_location, dim_supplier.location_id == dim_location.location_id) \
    .groupBy("country") \
    .agg(
        functions.sum("sale_total_price").alias("total_revenue"),
        functions.sum("sale_quantity").alias("total_quantity")
    ) \
    .orderBy(functions.col("total_revenue").desc())

# 6.1 Продукты с наивысшим и наименьшим рейтингом
report_6_1_top = dim_product \
    .select(
        "product_id",
        "product_name",
        "category",
        "price",
        "rating"
    ) \
    .withColumn("rank_type", functions.lit("top")) \
    .orderBy(functions.col("rating").desc()) \
    .limit(10)

report_6_1_bottom = dim_product \
    .select(
        "product_id",
        "product_name",
        "category",
        "price",
        "rating"
    ) \
    .withColumn("rank_type", functions.lit("bottom")) \
    .orderBy(functions.col("rating").asc()) \
    .limit(10)

report_6_1 = report_6_1_top.union(report_6_1_bottom)

# 6.2 Корреляция между рейтингом и объемом продаж
report_6_2 = fact_sales \
    .join(dim_product, "product_id") \
    .groupBy("product_id", "product_name", "rating") \
    .agg(functions.sum("sale_quantity").alias("total_quantity")) \
    .orderBy(functions.col("rating").desc(), functions.col("total_quantity").desc())

# 6.3 Продукты с наибольшим количеством отзывов
report_6_3 = dim_product \
    .select(
        "product_id",
        "product_name",
        "category",
        "price",
        functions.col("reviews").alias("total_reviews")
    ) \
    .orderBy(functions.col("total_reviews").desc())


write_to_clickhouse(showcase_1_1, "report_1_1_top_products")
write_to_clickhouse(showcase_1_2, "report_1_2_product_category_revenue")
write_to_clickhouse(showcase_1_3, "report_1_3_avg_products_rating")

write_to_clickhouse(showcase_2_1, "report_2_1_top_customers")
write_to_clickhouse(showcase_2_2, "report_2_2_customer_by_country")
write_to_clickhouse(showcase_2_3, "report_2_3_customers_avg_check")

write_to_clickhouse(showcase_3_1, "report_3_1_monthly_trends")
write_to_clickhouse(showcase_3_2, "report_3_2_seasonal_revenue")
write_to_clickhouse(showcase_3_3, "report_3_3_avg_order_by_month")

write_to_clickhouse(showcase_4_1, "report_4_1_top_stores")
write_to_clickhouse(showcase_4_2, "report_4_2_sales_by_city")
write_to_clickhouse(showcase_4_3, "report_4_3_store_avg_check")

write_to_clickhouse(showcase_5_1, "report_5_1_top_suppliers")
write_to_clickhouse(showcase_5_2, "report_5_2_supplier_avg_price")
write_to_clickhouse(showcase_5_3, "report_5_3_supplier_sales_by_country")

write_to_clickhouse(report_6_1, "report_6_1_product_rating_extremes")
write_to_clickhouse(report_6_2, "report_6_2_rating_sales_correlation")
write_to_clickhouse(report_6_3, "report_6_3_most_reviewed_products")


print("etl_clickhouse.py is done!")
spark.stop()