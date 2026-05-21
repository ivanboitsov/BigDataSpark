INSERT INTO dim_location (country, state, city, postal_code, location)
SELECT DISTINCT country, state, city, postal_code, location
FROM (
    -- customer location
    SELECT customer_country AS country, NULL AS state, NULL AS city, customer_postal_code AS postal_code, NULL AS location FROM mock_data
    UNION

    -- seller location
    SELECT seller_country AS country, NULL AS state, NULL AS city, seller_postal_code AS postal_code, NULL AS location FROM mock_data
    UNION

    -- store location
    SELECT store_country AS country, store_state AS state, store_city AS city, NULL AS postal_code, store_location AS location FROM mock_data
    UNION

    -- supplier location
    SELECT supplier_country AS country, NULL AS state, supplier_city AS city, NULL AS postal_code, NULL AS location FROM mock_data
) locations
WHERE country IS NOT NULL OR city IS NOT NULL OR state IS NOT NULL OR postal_code IS NOT NULL OR location IS NOT NULL
ON CONFLICT (country, city, state, postal_code, location) DO NOTHING;

INSERT INTO dim_pet (pet_type, pet_name, pet_breed)
SELECT DISTINCT 
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data
WHERE customer_pet_type IS NOT NULL
ON CONFLICT (pet_type, pet_name, pet_breed) DO NOTHING;


INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, pet_id, location_id)
SELECT DISTINCT ON (sale_customer_id)
    md.sale_customer_id,
    md.customer_first_name,
    md.customer_last_name,
    md.customer_age,
    md.customer_email,
    p.pet_id,
    l.location_id
FROM mock_data md
LEFT JOIN dim_location l
    ON l.country IS NOT DISTINCT FROM md.customer_country
    AND l.state IS NULL
    AND l.city IS NULL
    AND l.postal_code IS NOT DISTINCT FROM md.customer_postal_code
    AND l.location IS NULL
LEFT JOIN dim_pet p
    ON p.pet_type IS NOT DISTINCT FROM md.customer_pet_type
    AND p.pet_name IS NOT DISTINCT FROM md.customer_pet_name
    AND p.pet_breed IS NOT DISTINCT FROM md.customer_pet_breed
ON CONFLICT (customer_id) DO NOTHING;


INSERT INTO dim_seller (seller_id, first_name, last_name, email, location_id)
SELECT DISTINCT ON (sale_seller_id)
    md.sale_seller_id,
    md.seller_first_name,
    md.seller_last_name,
    md.seller_email,
    l.location_id
FROM mock_data md
LEFT JOIN dim_location l
    ON l.country IS NOT DISTINCT FROM md.seller_country
    AND l.state IS NULL
    AND l.city IS NULL
    AND l.postal_code IS NOT DISTINCT FROM md.seller_postal_code
    AND l.location IS NULL
ON CONFLICT (seller_id) DO NOTHING;


INSERT INTO dim_supplier (supplier_name, contact, email, phone, address, location_id)
SELECT DISTINCT ON (supplier_name)
    md.supplier_name,
    md.supplier_contact,
    md.supplier_email,
    md.supplier_phone,
    md.supplier_address,
    l.location_id
FROM mock_data md
LEFT JOIN dim_location l
    ON l.country IS NOT DISTINCT FROM md.supplier_country
    AND l.state IS NULL
    AND l.city IS NOT DISTINCT FROM md.supplier_city
    AND l.postal_code IS NULL
    AND l.location IS NULL
WHERE md.supplier_name IS NOT NULL
ON CONFLICT (supplier_name) DO NOTHING;


INSERT INTO dim_store (store_name, phone, email, location_id)
SELECT DISTINCT ON (store_name)
    md.store_name,
    md.store_phone,
    md.store_email,
    l.location_id
FROM mock_data md
LEFT JOIN dim_location l
    ON l.country IS NOT DISTINCT FROM md.store_country
    AND l.state IS NOT DISTINCT FROM md.store_state
    AND l.city IS NOT DISTINCT FROM md.store_city
    AND l.postal_code IS NULL
    AND l.location IS NOT DISTINCT FROM md.store_location
WHERE md.store_name IS NOT NULL
ON CONFLICT (store_name) DO NOTHING;


INSERT INTO dim_product (
    product_id,
    product_name,
    pet_category,
    category,
    quantity,
    price,
    weight,
    color,
    size,
    brand,
    material,
    description,
    rating,
    reviews,
    release_date,
    expiry_date,
    supplier_id
)
SELECT DISTINCT ON (sale_product_id)
    md.sale_product_id,
    md.product_name,
    md.pet_category,
    md.product_category,
    md.product_quantity,
    md.product_price,
    md.product_weight,
    md.product_color,
    md.product_size,
    md.product_brand,
    md.product_material,
    md.product_description,
    md.product_rating,
    md.product_reviews,
    md.product_release_date,
    md.product_expiry_date,
    s.supplier_id
FROM mock_data md
LEFT JOIN dim_supplier s ON s.supplier_name = md.supplier_name
ON CONFLICT (product_id) DO NOTHING;
 

INSERT INTO fact_sales (
    customer_id,
    seller_id,
    product_id,
    store_id,
    sale_date,
    sale_quantity,
    sale_total_price,
    product_price
)
SELECT
    md.sale_customer_id,
    md.sale_seller_id,
    md.sale_product_id,
    st.store_id,
    md.sale_date,
    md.sale_quantity,
    md.sale_total_price,
    md.product_price
FROM mock_data md
LEFT JOIN dim_store st
    ON st.store_name = md.store_name;
