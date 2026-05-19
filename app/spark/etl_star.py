from pyspark.sql import SparkSession
from app.core.config import PG_URL, PG_PROPS, PG_JAR, CH_JAR


spark = SparkSession.buildder.appName("ETL PetShop Star Schema").getOrCreate()

def read_table(table_name):
    return spark.read.jdbc(url=PG_URL, table=table_name, properties=PG_PROPS)

def write_table(df, table_name):
    df.write.jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS)

# ==================== SPARK ====================
spark = SparkSession.builder \
    .appName("ETL: star schema -> ClickHouse") \
    .config("spark.jars", f"{PG_JAR},{CH_JAR}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
