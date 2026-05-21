#!/bin/bash
export $(grep -v '^#' .env | xargs)

# Delete existing containers and volumes
docker compose down -v

# Start containers
echo "Starting containers..."
docker compose up -d

# Wait for Postgres
echo "Waiting for Postgres to be ready..."
until docker exec petshop_postgres pg_isready -U postgres -d petshop_db > /dev/null 2>&1; do
    sleep 2
done
echo "Postgres is ready!"

# Wait for Postgres init scripts
echo "Waiting for Postgres initialization..."
until docker logs petshop_postgres 2>&1 | grep -q "PostgreSQL init process complete"; do
    sleep 2
done
echo "Postgres initialization complete!"

# Wait for Spark ETL postgres to finish
echo "Waiting for etl_postgres.py to complete..."
until docker inspect petshop_spark_postgres --format='{{.State.Status}}' | grep -q "exited"; do
    sleep 5
done
echo "etl_postgres.py complete!"

# Wait for Spark ETL clickhouse to finish
echo "Waiting for etl_clickhouse.py to complete..."
until docker inspect petshop_spark_clickhouse --format='{{.State.Status}}' | grep -q "exited"; do
    sleep 5
done
echo "etl_clickhouse.py complete!"

# Check Spark logs for errors
echo "Checking Spark logs..."
docker logs petshop_spark_postgres 2>&1 | grep -i "error" || echo "No errors in etl_postgres"
docker logs petshop_spark_clickhouse 2>&1 | grep -i "error" || echo "No errors in etl_clickhouse"

# Check PostgreSQL tables
echo "PostgreSQL tables:"
docker exec petshop_postgres psql -U postgres -d petshop_db -c "\dt"

echo "PostgreSQL row counts:"
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

echo "fact_sales sample:"
docker exec petshop_postgres psql -U postgres -d petshop_db -c "SELECT * FROM fact_sales LIMIT 10;"

# Check ClickHouse tables
echo "ClickHouse tables:"
docker exec petshop_clickhouse clickhouse-client \
    --user "${CH_USER}" \
    --password "${CH_PASSWORD}" \
    --database "${CH_DB}" \
    --query "SHOW TABLES"

echo "ClickHouse row counts:"
docker exec petshop_clickhouse clickhouse-client \
    --user "${CH_USER}" \
    --password "${CH_PASSWORD}" \
    --database "${CH_DB}" \
    --query "
SELECT table, sum(rows) AS row_count
FROM system.parts
WHERE database = '${CH_DB}' AND active
GROUP BY table
ORDER BY table;
"