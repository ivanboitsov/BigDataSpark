import os
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

CH_HOST = os.getenv("CH_HOST")
CH_PORT = os.getenv("CH_PORT")
CH_DB = os.getenv("CH_DB")
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")

PG_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"
CH_JAR = "/path/to/clickhouse-jdbc.jar"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
CH_PROPS = {
    "user": CH_USER,
    "password": CH_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}
