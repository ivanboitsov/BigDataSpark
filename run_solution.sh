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
echo "ClickHouse tables and row counts:"
docker exec petshop_clickhouse clickhouse-client \
    --user "${CH_USER}" \
    --password "${CH_PASSWORD}" \
    --database "${CH_DB}" \
    --query "
SELECT
    table,
    sum(rows) AS row_count,
    formatReadableSize(sum(bytes)) AS size
FROM system.parts
WHERE database = '${CH_DB}' AND active
GROUP BY table
ORDER BY table;
"

echo ""
echo "Sample data from each report:"

for table in \
    report_1_1_top_products \
    report_1_2_product_category_revenue \
    report_1_3_avg_products_rating \
    report_2_1_top_customers \
    report_2_2_customer_by_country \
    report_2_3_customers_avg_check \
    report_3_1_monthly_trends \
    report_3_2_seasonal_revenue \
    report_3_3_avg_order_by_month \
    report_4_1_top_stores \
    report_4_2_sales_by_city \
    report_4_3_store_avg_check \
    report_5_1_top_suppliers \
    report_5_2_supplier_avg_price \
    report_5_3_supplier_sales_by_country \
    report_6_1_product_rating_extremes \
    report_6_2_rating_sales_correlation \
    report_6_3_most_reviewed_products; do
    echo "--- $table ---"
    docker exec petshop_clickhouse clickhouse-client \
        --user "${CH_USER}" \
        --password "${CH_PASSWORD}" \
        --database "${CH_DB}" \
        --query "SELECT * FROM ${table} LIMIT 3 FORMAT PrettyCompact"
    echo ""
done