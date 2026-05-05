#!/bin/bash

# Delete existing container and volumes
docker compose down -v

# Start the container
echo "Starting the container..."
docker compose up -d

# Wait for Postgres to be ready
echo "Waiting for Postgres to be ready..."
until docker exec petshop_snowflake pg_isready -U postgres -d petshop_db > /dev/null 2>&1; do
    sleep 2
done
echo "Postgres is ready!"

# Wait for init scripts to complete
echo "Waiting for initialization scripts to complete..."
until docker logs petshop_snowflake 2>&1 | grep -q "PostgreSQL init process complete"; do
    sleep 2
done
echo "Initialization complete!"

# Check logs for erors
docker lgogs petshop_snowflake 2>&1 | grep -i "error" || echo "No errors found in logs."

# Check tables and data
docker exec -it petshop_snowflake psql -U postgres -d petshop_db -c "\dt"
docker exec petshop_snowflake psql -U postgres -d petshop_db -c "
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

docker exec -it petshop_snowflake psql -U postgres -d petshop_db -c "SELECT * FROM fact_sales LIMIT 10;"
