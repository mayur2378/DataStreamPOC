-- Clears target tables directly via TRUNCATE (bypasses CDC).
-- Use this to reset the target independently without touching the source.
--
-- Usage: psql "host=<endpoint> dbname=pocdb user=pocadmin password=<pwd>" -f sql/clear_target.sql

SET search_path = target;

TRUNCATE order_items, inventory, orders, products, customers RESTART IDENTITY CASCADE;

\echo 'Target tables cleared (direct TRUNCATE, no CDC involved).'
