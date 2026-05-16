-- Run once after CDK deploy to create both schemas and all tables.
-- psql "host=<endpoint> dbname=pocdb user=pocadmin password=<pwd>" -f sql/schema.sql

CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS target;

-- ── Source tables ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS source.customers (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source.products (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  category   TEXT,
  price      NUMERIC(10, 2),
  stock_qty  INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source.orders (
  id           SERIAL PRIMARY KEY,
  customer_id  INT,
  status       TEXT NOT NULL DEFAULT 'pending',
  total_amount NUMERIC(12, 2),
  order_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source.order_items (
  id         SERIAL PRIMARY KEY,
  order_id   INT,
  product_id INT,
  quantity   INT NOT NULL DEFAULT 1,
  unit_price NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS source.inventory (
  id           SERIAL PRIMARY KEY,
  product_id   INT,
  warehouse_id TEXT,
  quantity     INT NOT NULL DEFAULT 0,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Target tables (mirror of source, no FK constraints for upsert simplicity) ─

CREATE TABLE IF NOT EXISTS target.customers  (LIKE source.customers  INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.products   (LIKE source.products   INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.orders     (LIKE source.orders     INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.order_items(LIKE source.order_items INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.inventory  (LIKE source.inventory  INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

\echo 'Schema created: source and target schemas with 5 tables each.'
