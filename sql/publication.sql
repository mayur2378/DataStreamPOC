-- Run once after schema.sql.
-- Creates the PostgreSQL logical replication publication that Debezium reads from.

CREATE PUBLICATION poc_publication
  FOR TABLE
    source.customers,
    source.products,
    source.orders,
    source.order_items,
    source.inventory;

\echo 'Publication poc_publication created on source schema tables.'
