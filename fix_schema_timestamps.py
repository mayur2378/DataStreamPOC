import os, json, boto3, psycopg2

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()

alters = [
    "ALTER TABLE source.customers  ALTER COLUMN created_at  TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC'",
    "ALTER TABLE source.customers  ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE source.products   ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE source.orders     ALTER COLUMN order_date  TYPE TIMESTAMP USING order_date AT TIME ZONE 'UTC'",
    "ALTER TABLE source.orders     ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE source.inventory  ALTER COLUMN last_updated TYPE TIMESTAMP USING last_updated AT TIME ZONE 'UTC'",
    "ALTER TABLE target.customers  ALTER COLUMN created_at  TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC'",
    "ALTER TABLE target.customers  ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE target.products   ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE target.orders     ALTER COLUMN order_date  TYPE TIMESTAMP USING order_date AT TIME ZONE 'UTC'",
    "ALTER TABLE target.orders     ALTER COLUMN updated_at  TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC'",
    "ALTER TABLE target.inventory  ALTER COLUMN last_updated TYPE TIMESTAMP USING last_updated AT TIME ZONE 'UTC'",
]

for sql in alters:
    cur.execute(sql)
    print(f"OK: {sql[:60]}...")

# Drop replication slots so Debezium re-snapshots with the new column types
cur.execute("SELECT slot_name FROM pg_replication_slots")
slots = [r[0] for r in cur.fetchall()]
for slot in slots:
    cur.execute(f"SELECT pg_drop_replication_slot('{slot}')")
    print(f"Dropped slot: {slot}")

conn.close()
print("\nDone. Now recreate both connectors.")
