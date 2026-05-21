from pyspark.sql import SparkSession
from pyspark.sql import functions

from core.config import PG_URL, PG_PROPS, PG_JAR


spark = SparkSession.builder \
    .appName("ETL: mock_data -> star schema") \
    .config("spark.jars", PG_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Reading mock_data...")
df = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)


# --- dim_location ---
customer_loc = df.select(
    functions.col("customer_country").alias("country"),
    functions.lit(None).cast("string").alias("state"),
    functions.lit(None).cast("string").alias("city"),
    functions.col("customer_postal_code").alias("postal_code"),
    functions.lit(None).cast("string").alias("location")
)
seller_loc = df.select(
    functions.col("seller_country").alias("country"),
    functions.lit(None).cast("string").alias("state"),
    functions.lit(None).cast("string").alias("city"),
    functions.col("seller_postal_code").alias("postal_code"),
    functions.lit(None).cast("string").alias("location")
)

store_loc = df.select(
    functions.col("store_country").alias("country"),
    functions.col("store_state").alias("state"),
    functions.col("store_city").alias("city"),
    functions.lit(None).cast("string").alias("postal_code"),
    functions.col("store_location").alias("location")
)

supplier_loc = df.select(
    functions.col("supplier_country").alias("country"),
    functions.lit(None).cast("string").alias("state"),
    functions.lit("supplier_city").cast("string").alias("city"),
    functions.lit(None).cast("string").alias("postal_code"),
    functions.lit(None).cast("string").alias("location")
)

dim_location = customer_loc.union(seller_loc).union(store_loc).union(supplier_loc) \
    .distinct() \
    .filter(
        functions.col("country").isNotNull() |
        functions.col("city").isNotNull() |
        functions.col("state").isNotNull() |
        functions.col("postal_code").isNotNull() |
        functions.col("location").isNotNull()
    )

dim_location.write.jdbc(url=PG_URL, table="dim_location", mode="append", properties=PG_PROPS)
loc_df = spark.read.jdbc(url=PG_URL, table="dim_location", properties=PG_PROPS)


# --- dim_pet ---
dim_pet = df.select(
    functions.col("customer_pet_type").alias("pet_type"),
    functions.col("customer_pet_name").alias("pet_name"),
    functions.col("customer_pet_breed").alias("pet_breed")
).distinct().filter(functions.col("pet_type").isNotNull())

dim_pet.write.jdbc(url=PG_URL, table="dim_pet", mode="append", properties=PG_PROPS)
pet_df = spark.read.jdbc(url=PG_URL, table="dim_pet", properties=PG_PROPS)


# --- dim_customer ---
cust_loc_df = loc_df.filter(
    functions.col("state").isNull() &
    functions.col("city").isNull() &
    functions.col("location").isNull()
)

dim_customer = df.select(
    functions.col("sale_customer_id").alias("customer_id"),
    functions.col("customer_first_name").alias("first_name"),
    functions.col("customer_last_name").alias("last_name"),
    functions.col("customer_age").alias("age"),
    functions.col("customer_email").alias("email"),
    functions.col("customer_country"),
    functions.col("customer_postal_code"),
    functions.col("customer_pet_type"),
    functions.col("customer_pet_name"),
    functions.col("customer_pet_breed")
).dropDuplicates(["customer_id"]) \
    .join(
        cust_loc_df.select(
            functions.col("location_id"),
            functions.col("country").alias("loc_country"),
            functions.col("postal_code").alias("loc_postal")
        ),
        (functions.col("customer_country") == functions.col("loc_country")) &
        (functions.col("customer_postal_code") == functions.col("loc_postal")),
        "left"
    ) \
    .join(
        pet_df,
        (functions.col("customer_pet_type") == pet_df.pet_type) &
        (functions.col("customer_pet_name") == pet_df.pet_name) &
        (functions.col("customer_pet_breed") == pet_df.pet_breed),
        "left"
    ) \
    .select("customer_id", "first_name", "last_name", "age", "email", "pet_id", "location_id")
dim_customer.write.jdbc(url=PG_URL, table="dim_customer", mode="append", properties=PG_PROPS)


# --- dim_seller ---
dim_seller = df.select(
    functions.col("sale_seller_id").alias("seller_id"),
    functions.col("seller_first_name").alias("first_name"),
    functions.col("seller_last_name").alias("last_name"),
    functions.col("seller_email").alias("email"),
    functions.col("seller_country"),
    functions.col("seller_postal_code")
).dropDuplicates(["seller_id"]) \
    .join(
        cust_loc_df.select(
            functions.col("location_id"),
            functions.col("country").alias("loc_country"),
            functions.col("postal_code").alias("loc_postal")
        ),
        (functions.col("seller_country") == functions.col("loc_country")) &
        (functions.col("seller_postal_code") == functions.col("loc_postal")),
        "left"
    ) \
    .select("seller_id", "first_name", "last_name", "email", "location_id")
dim_seller.write.jdbc(url=PG_URL, table="dim_seller", mode="append", properties=PG_PROPS)


# --- dim_supplier ---
sup_loc_df = loc_df.filter(
    functions.col("state").isNull() &
    functions.col("postal_code").isNull() &
    functions.col("location").isNull()
)

dim_supplier = df.select(
    functions.col("supplier_name"),
    functions.col("supplier_contact").alias("contact"),
    functions.col("supplier_email").alias("email"),
    functions.col("supplier_phone").alias("phone"),
    functions.col("supplier_address").alias("address"),
    functions.col("supplier_country"),
    functions.col("supplier_city")
).dropDuplicates(["supplier_name"]) \
    .filter(functions.col("supplier_name").isNotNull()) \
    .join(
        sup_loc_df.select(
            functions.col("location_id"),
            functions.col("country").alias("loc_country"),
            functions.col("city").alias("loc_city")
        ),
        (functions.col("supplier_country") == functions.col("loc_country")) &
        (functions.col("supplier_city") == functions.col("loc_city")),
        "left"
    ) \
    .select("supplier_name", "contact", "email", "phone", "address", "location_id")
dim_supplier.write.jdbc(url=PG_URL, table="dim_supplier", mode="append", properties=PG_PROPS)
sup_df = spark.read.jdbc(url=PG_URL, table="dim_supplier", properties=PG_PROPS)


# --- dim_store ---
store_loc_df = loc_df.filter(functions.col("postal_code").isNull())

dim_store = df.select(
    functions.col("store_name"),
    functions.col("store_phone").alias("phone"),
    functions.col("store_email").alias("email"),
    functions.col("store_country"),
    functions.col("store_state"),
    functions.col("store_city"),
    functions.col("store_location")
).dropDuplicates(["store_name"]) \
    .filter(functions.col("store_name").isNotNull()) \
    .join(
        store_loc_df.select(
            functions.col("location_id"),
            functions.col("country").alias("loc_country"),
            functions.col("state").alias("loc_state"),
            functions.col("city").alias("loc_city"),
            functions.col("location").alias("loc_location")
        ),
        (functions.col("store_country") == functions.col("loc_country")) &
        (functions.col("store_state") == functions.col("loc_state")) &
        (functions.col("store_city") == functions.col("loc_city")) &
        (functions.col("store_location") == functions.col("loc_location")),
        "left"
    ) \
    .select("store_name", "phone", "email", "location_id")
dim_store.write.jdbc(url=PG_URL, table="dim_store", mode="append", properties=PG_PROPS)
store_df = spark.read.jdbc(url=PG_URL, table="dim_store", properties=PG_PROPS)


# --- dim_product ---
dim_product = df.select(
    functions.col("sale_product_id").alias("product_id"),
    functions.col("product_name"),
    functions.col("pet_category"),
    functions.col("product_category").alias("category"),
    functions.col("product_quantity").alias("quantity"),
    functions.col("product_price").alias("price"),
    functions.col("product_weight").alias("weight"),
    functions.col("product_color").alias("color"),
    functions.col("product_size").alias("size"),
    functions.col("product_brand").alias("brand"),
    functions.col("product_material").alias("material"),
    functions.col("product_description").alias("description"),
    functions.col("product_rating").alias("rating"),
    functions.col("product_reviews").alias("reviews"),
    functions.col("product_release_date").alias("release_date"),
    functions.col("product_expiry_date").alias("expiry_date"),
    functions.col("supplier_name")
).dropDuplicates(["product_id"]) \
    .join(
        sup_df.select(
            functions.col("supplier_id"),
            functions.col("supplier_name").alias("sup_name")
        ),
        functions.col("supplier_name") == functions.col("sup_name"),
        "left"
    ) \
    .select("product_id", "product_name", "pet_category", "category",
            "quantity", "price", "weight", "color", "size", "brand",
            "material", "description", "rating", "reviews",
            "release_date", "expiry_date", "supplier_id")
dim_product.write.jdbc(url=PG_URL, table="dim_product", mode="append", properties=PG_PROPS)


# --- fact_sales ---
fact_sales = df.select(
    functions.col("sale_customer_id").alias("customer_id"),
    functions.col("sale_seller_id").alias("seller_id"),
    functions.col("sale_product_id").alias("product_id"),
    functions.col("store_name"),
    functions.col("sale_date"),
    functions.col("sale_quantity"),
    functions.col("sale_total_price"),
    functions.col("product_price")
).join(
    store_df.select(
        functions.col("store_id"),
        functions.col("store_name").alias("st_name")
    ),
    functions.col("store_name") == functions.col("st_name"),
    "left"
).select("customer_id", "seller_id", "product_id", "store_id",
         "sale_date", "sale_quantity", "sale_total_price", "product_price")
fact_sales.write.jdbc(url=PG_URL, table="fact_sales", mode="append", properties=PG_PROPS)


print("etl_postgres.py is done!")
spark.stop()