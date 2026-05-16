-- Clears all source tables using DELETE so each row removal flows through
-- Debezium CDC → MSK → target. Wait ~15s after running before reloading.
--
-- Usage: psql "host=<endpoint> dbname=pocdb user=pocadmin password=<pwd>" -f sql/clear_source.sql

SET search_path = source;

DELETE FROM order_items;
DELETE FROM inventory;
DELETE FROM orders;
DELETE FROM products;
DELETE FROM customers;

-- Reset sequences so IDs restart from 1 on next seed
ALTER SEQUENCE customers_id_seq  RESTART WITH 1;
ALTER SEQUENCE products_id_seq   RESTART WITH 1;
ALTER SEQUENCE orders_id_seq     RESTART WITH 1;
ALTER SEQUENCE order_items_id_seq RESTART WITH 1;
ALTER SEQUENCE inventory_id_seq  RESTART WITH 1;

\echo 'Source tables cleared. Wait ~15s for CDC to propagate deletes to target.'
