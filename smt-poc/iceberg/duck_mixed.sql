INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET minio_s3 (TYPE s3, KEY_ID 'admin', SECRET 'password', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 'warehouse' AS ice (TYPE iceberg, ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none');
SELECT order_id, doc.customer.addr.city AS city, doc.customer.addr.geo.lat AS lat, doc.status AS status FROM ice.demo.arrow_variant_shred ORDER BY order_id;
SELECT count(*) AS rows_total, count(*) FILTER (WHERE doc.customer.addr.city = 'pune') AS pune FROM ice.demo.arrow_variant_shred;
