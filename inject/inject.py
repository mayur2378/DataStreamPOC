"""
inject.py — Data injection CLI for the DataStream POC.

Connects to RDS PostgreSQL and drives the full POC lifecycle without any
external tools (no psql, no DB client required).

Setup:
    pip install -r requirements.txt
    export DB_SECRET_ARN=<arn>      # from CDK output DataStreamDbSecretArn
    export AWS_REGION=<region>       # e.g. us-east-1
    export DB_HOST=<rds-endpoint>    # from CDK output DataStreamDbEndpoint

First-time setup (run once after CDK deploy + connector setup):
    python inject.py init            # creates schemas, tables, publication

Daily workflow:
    python inject.py seed            # insert 20 rows per table
    python inject.py status          # compare source vs target counts
    python inject.py bulk --table orders --count 5000
    python inject.py bulk-all --count 1000
    python inject.py chaos --rate 5 --duration 30

Reset for a new test run:
    python inject.py clear-source    # delete source rows (CDC propagates to target)
    python inject.py clear-target    # truncate target directly
    python inject.py clear-all       # both in sequence with wait
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from contextlib import contextmanager
from typing import Generator

import boto3
import click
import psycopg2
import psycopg2.extras
from faker import Faker
from tabulate import tabulate

fake = Faker()

TABLES = ["customers", "products", "orders", "order_items", "inventory"]

# ── Connection ─────────────────────────────────────────────────────────────────

def _get_secret() -> dict:
    arn = os.environ.get("DB_SECRET_ARN")
    if not arn:
        raise SystemExit("DB_SECRET_ARN environment variable is not set.")
    region = os.environ.get("AWS_REGION", "us-east-1")
    sm = boto3.client("secretsmanager", region_name=region)
    return json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])


@contextmanager
def get_conn(schema: str | None = "source") -> Generator[psycopg2.extensions.connection, None, None]:
    secret = _get_secret()
    host = os.environ.get("DB_HOST") or secret.get("host")
    options = f"-c search_path={schema}" if schema else ""
    conn = psycopg2.connect(
        host=host,
        port=int(secret.get("port", 5432)),
        dbname=secret.get("dbname", "pocdb"),
        user=secret["username"],
        password=secret["password"],
        options=options,
        connect_timeout=10,
    )
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Row generators ─────────────────────────────────────────────────────────────

def _customer_row() -> dict:
    return {
        "name": fake.name(),
        "email": fake.unique.email(),
        "status": random.choice(["active", "inactive", "pending"]),
    }


def _product_row() -> dict:
    return {
        "name": fake.catch_phrase(),
        "category": random.choice(["Electronics", "Clothing", "Food", "Tools", "Books"]),
        "price": round(random.uniform(1.99, 999.99), 2),
        "stock_qty": random.randint(0, 500),
    }


def _order_row(cursor: psycopg2.extensions.cursor) -> dict | None:
    cursor.execute("SELECT id FROM customers ORDER BY random() LIMIT 1")
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "customer_id": row[0],
        "status": random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
        "total_amount": round(random.uniform(10.0, 2000.0), 2),
    }


def _order_item_row(cursor: psycopg2.extensions.cursor) -> dict | None:
    cursor.execute("SELECT id FROM orders ORDER BY random() LIMIT 1")
    order = cursor.fetchone()
    cursor.execute("SELECT id FROM products ORDER BY random() LIMIT 1")
    product = cursor.fetchone()
    if not order or not product:
        return None
    return {
        "order_id": order[0],
        "product_id": product[0],
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(1.99, 499.99), 2),
    }


def _inventory_row(cursor: psycopg2.extensions.cursor) -> dict | None:
    cursor.execute("SELECT id FROM products ORDER BY random() LIMIT 1")
    product = cursor.fetchone()
    if not product:
        return None
    return {
        "product_id": product[0],
        "warehouse_id": f"WH-{random.randint(1, 10):02d}",
        "quantity": random.randint(0, 1000),
    }


_ROW_GENERATORS = {
    "customers": lambda cur: _customer_row(),
    "products": lambda cur: _product_row(),
    "orders": _order_row,
    "order_items": _order_item_row,
    "inventory": _inventory_row,
}


def _insert_row(cursor: psycopg2.extensions.cursor, table: str) -> bool:
    row = _ROW_GENERATORS[table](cursor)
    if row is None:
        return False
    cols = ", ".join(row.keys())
    placeholders = ", ".join(["%s"] * len(row))
    cursor.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
        list(row.values()),
    )
    return True


# ── CLI ────────────────────────────────────────────────────────────────────────

@click.group()
def cli() -> None:
    """DataStream POC — inject data into the source RDS schema."""


_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS target;

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
  price      NUMERIC(10,2),
  stock_qty  INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS source.orders (
  id           SERIAL PRIMARY KEY,
  customer_id  INT,
  status       TEXT NOT NULL DEFAULT 'pending',
  total_amount NUMERIC(12,2),
  order_date   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS source.order_items (
  id         SERIAL PRIMARY KEY,
  order_id   INT,
  product_id INT,
  quantity   INT NOT NULL DEFAULT 1,
  unit_price NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS source.inventory (
  id           SERIAL PRIMARY KEY,
  product_id   INT,
  warehouse_id TEXT,
  quantity     INT NOT NULL DEFAULT 0,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS target.customers   (LIKE source.customers   INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.products    (LIKE source.products    INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.orders      (LIKE source.orders      INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.order_items (LIKE source.order_items INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS target.inventory   (LIKE source.inventory   INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
"""

_PUBLICATION_DDL = """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'poc_publication') THEN
    CREATE PUBLICATION poc_publication
      FOR TABLE source.customers, source.products, source.orders,
                source.order_items, source.inventory;
  END IF;
END $$;
"""


@cli.command()
def init() -> None:
    """Create schemas, tables, and Debezium publication. Run once after CDK deploy."""
    click.echo("Creating schemas and tables...")
    with get_conn(schema=None) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for statement in _SCHEMA_DDL.strip().split(";"):
                s = statement.strip()
                if s:
                    cur.execute(s)
        conn.autocommit = False

    click.echo("Creating Debezium publication (poc_publication)...")
    with get_conn(schema=None) as conn:
        with conn.cursor() as cur:
            cur.execute(_PUBLICATION_DDL)

    click.echo("Done. Run: python inject.py seed")


def _do_clear_source() -> None:
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            for table in ["order_items", "inventory", "orders", "products", "customers"]:
                cur.execute(f"DELETE FROM {table}")
                cur.execute(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1")


def _do_clear_target() -> None:
    with get_conn("target") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE order_items, inventory, orders, products, customers RESTART IDENTITY CASCADE"
            )


@cli.command("clear-source")
def clear_source() -> None:
    """Delete all source rows (propagates to target via CDC). Wait ~15s before reloading."""
    click.echo("Clearing source tables via DELETE (CDC will propagate to target)...")
    _do_clear_source()
    click.echo("Source cleared. Wait ~15s for CDC propagation, then verify with: python inject.py status")


@cli.command("clear-target")
def clear_target() -> None:
    """Truncate target tables directly (bypasses CDC). Use to reset target independently."""
    click.echo("Truncating target tables directly...")
    _do_clear_target()
    click.echo("Target cleared.")


@cli.command("clear-all")
def clear_all() -> None:
    """Clear source (CDC-propagated) then force-clear target. Waits 20s for propagation."""
    click.echo("Clearing source tables via DELETE (CDC will propagate to target)...")
    _do_clear_source()
    click.echo("Waiting 20s for CDC propagation...")
    time.sleep(20)
    click.echo("Force-clearing target tables...")
    _do_clear_target()
    click.echo("Both schemas cleared. Run: python inject.py seed")


@cli.command()
def seed() -> None:
    """Insert 20 rows per table using Faker data."""
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            for table in TABLES:
                inserted = 0
                for _ in range(20):
                    if _insert_row(cur, table):
                        inserted += 1
                click.echo(f"  {table}: +{inserted} rows")
    click.echo("Seed complete. Wait ~10s then run: python inject.py status")


@cli.command()
@click.option("--table", required=True, type=click.Choice(TABLES), help="Target table")
def insert(table: str) -> None:
    """Insert one random row into TABLE."""
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            ok = _insert_row(cur, table)
    if ok:
        click.echo(f"Inserted 1 row into source.{table}")
    else:
        click.echo(f"Skipped: prerequisite rows missing for {table} (run seed first)")


@cli.command()
@click.option("--table", required=True, type=click.Choice(TABLES), help="Target table")
@click.option("--count", default=1000, show_default=True, help="Number of rows to insert")
@click.option("--batch-size", default=500, show_default=True, help="Rows per executemany batch")
def bulk(table: str, count: int, batch_size: int) -> None:
    """Bulk-insert COUNT rows into TABLE using batched executemany."""
    if table in ("orders", "order_items", "inventory"):
        click.echo(f"Note: {table} depends on parent rows — seeding customers/products first if needed.")

    total = 0
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            batch: list[dict] = []
            for i in range(count):
                row = _ROW_GENERATORS[table](cur)
                if row is None:
                    continue
                batch.append(row)
                if len(batch) >= batch_size:
                    cols = list(batch[0].keys())
                    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
                    psycopg2.extras.execute_batch(cur, sql, [list(r.values()) for r in batch])
                    total += len(batch)
                    batch = []
                    click.echo(f"  {total}/{count} rows inserted...", nl=False)
                    click.echo("\r", nl=False)
            if batch:
                cols = list(batch[0].keys())
                sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
                psycopg2.extras.execute_batch(cur, sql, [list(r.values()) for r in batch])
                total += len(batch)

    click.echo(f"Bulk insert complete: {total} rows into source.{table}")


@cli.command("bulk-all")
@click.option("--count", default=1000, show_default=True, help="Rows per table")
@click.option("--batch-size", default=500, show_default=True)
def bulk_all(count: int, batch_size: int) -> None:
    """Bulk-insert COUNT rows into all 5 tables concurrently (threaded)."""

    results: dict[str, int] = {}
    errors: dict[str, str] = {}

    def _worker(table: str) -> None:
        try:
            total = 0
            with get_conn("source") as conn:
                with conn.cursor() as cur:
                    batch: list[dict] = []
                    for _ in range(count):
                        row = _ROW_GENERATORS[table](cur)
                        if row is None:
                            continue
                        batch.append(row)
                        if len(batch) >= batch_size:
                            cols = list(batch[0].keys())
                            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
                            psycopg2.extras.execute_batch(cur, sql, [list(r.values()) for r in batch])
                            total += len(batch)
                            batch = []
                    if batch:
                        cols = list(batch[0].keys())
                        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
                        psycopg2.extras.execute_batch(cur, sql, [list(r.values()) for r in batch])
                        total += len(batch)
            results[table] = total
        except Exception as exc:
            errors[table] = str(exc)

    # Seed customers and products first (dependencies for other tables)
    for prereq in ["customers", "products"]:
        _worker(prereq)

    threads = [threading.Thread(target=_worker, args=(t,)) for t in ["orders", "order_items", "inventory"]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for table, n in results.items():
        click.echo(f"  source.{table}: +{n} rows")
    for table, err in errors.items():
        click.echo(f"  source.{table}: ERROR — {err}", err=True)


@cli.command()
@click.option("--table", required=True, type=click.Choice(TABLES))
def update(table: str) -> None:
    """Update a random existing row in TABLE."""
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                click.echo(f"No rows in source.{table} — run seed first.")
                return
            rid = row[0]
            if table == "customers":
                cur.execute("UPDATE customers SET status=%s, updated_at=now() WHERE id=%s",
                            (random.choice(["active", "inactive"]), rid))
            elif table == "products":
                cur.execute("UPDATE products SET price=%s, stock_qty=%s, updated_at=now() WHERE id=%s",
                            (round(random.uniform(1.99, 999.99), 2), random.randint(0, 500), rid))
            elif table == "orders":
                cur.execute("UPDATE orders SET status=%s, updated_at=now() WHERE id=%s",
                            (random.choice(["processing", "shipped", "delivered", "cancelled"]), rid))
            elif table == "order_items":
                cur.execute("UPDATE order_items SET quantity=%s WHERE id=%s",
                            (random.randint(1, 20), rid))
            elif table == "inventory":
                cur.execute("UPDATE inventory SET quantity=%s, last_updated=now() WHERE id=%s",
                            (random.randint(0, 1000), rid))
    click.echo(f"Updated row id={rid} in source.{table}")


@cli.command()
@click.option("--table", required=True, type=click.Choice(TABLES))
def delete(table: str) -> None:
    """Delete a random row from TABLE."""
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                click.echo(f"No rows in source.{table}.")
                return
            rid = row[0]
            cur.execute(f"DELETE FROM {table} WHERE id = %s", (rid,))
    click.echo(f"Deleted row id={rid} from source.{table}")


@cli.command()
@click.option("--rate", default=5, show_default=True, help="Operations per second")
@click.option("--duration", default=30, show_default=True, help="Duration in seconds")
def chaos(rate: int, duration: int) -> None:
    """Fire a random mix of INSERT/UPDATE/DELETE at RATE ops/sec for DURATION seconds."""
    ops = ["insert", "insert", "insert", "update", "update", "delete"]
    end_time = time.time() + duration
    interval = 1.0 / rate
    total = 0

    click.echo(f"Starting chaos: {rate} ops/sec for {duration}s (Ctrl+C to stop)")
    with get_conn("source") as conn:
        with conn.cursor() as cur:
            while time.time() < end_time:
                t_start = time.time()
                table = random.choice(TABLES)
                op = random.choice(ops)
                try:
                    if op == "insert":
                        _insert_row(cur, table)
                    elif op == "update":
                        cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
                        r = cur.fetchone()
                        if r:
                            cur.execute(f"UPDATE {table} SET updated_at=now() WHERE id=%s", (r[0],))
                    elif op == "delete":
                        cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
                        r = cur.fetchone()
                        if r:
                            cur.execute(f"DELETE FROM {table} WHERE id=%s", (r[0],))
                    conn.commit()
                    total += 1
                except Exception as exc:
                    conn.rollback()
                    click.echo(f"\n  warn: {exc}")

                elapsed = time.time() - t_start
                sleep_for = max(0.0, interval - elapsed)
                if sleep_for > 0:
                    time.sleep(sleep_for)

                remaining = int(end_time - time.time())
                click.echo(f"  {total} ops fired — {remaining}s remaining    \r", nl=False)

    click.echo(f"\nChaos complete: {total} operations in {duration}s")


@cli.command()
def status() -> None:
    """Show row counts on source vs target for each table."""
    rows = []
    with get_conn("source") as src_conn, get_conn("target") as tgt_conn:
        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
            for table in TABLES:
                src_cur.execute(f"SELECT COUNT(*) FROM {table}")
                tgt_cur.execute(f"SELECT COUNT(*) FROM {table}")
                src_count = src_cur.fetchone()[0]
                tgt_count = tgt_cur.fetchone()[0]
                diff = src_count - tgt_count
                lag = f"+{diff}" if diff > 0 else str(diff)
                rows.append([table, src_count, tgt_count, lag])

    click.echo(tabulate(rows, headers=["Table", "Source", "Target", "Lag"], tablefmt="rounded_outline"))


if __name__ == "__main__":
    cli()
