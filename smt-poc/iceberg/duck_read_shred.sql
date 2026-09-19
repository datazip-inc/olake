INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET minio_s3 (TYPE s3, KEY_ID 'admin', SECRET 'password', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 'warehouse' AS ice (TYPE iceberg, ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none');
SELECT order_id, customer_email, doc.customer.addr.city AS city, doc.items[1].sku AS first_sku, doc.customer.addr.geo.lat AS lat_residual, variant_typeof(doc) AS t FROM ice.demo.arrow_variant_shred ORDER BY order_id;
