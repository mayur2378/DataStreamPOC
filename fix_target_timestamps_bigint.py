import os, json, boto3, psycopg2

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()

ts_columns = [
    ("target.customers", "created_at"),
    ("target.customers", "updated_at"),
    ("target.products",  "updated_at"),
    ("target.orders",    "order_date"),
    ("target.orders",    "updated_at"),
    ("target.inventory", "last_updated"),
]

for table, col in ts_columns:
    cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} DROP DEFAULT")
    cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} TYPE BIGINT USING EXTRACT(EPOCH FROM {col})::BIGINT * 1000")
    print(f"OK: {table}.{col} → BIGINT")

conn.close()
print("\nDone. Recreate the sink connector to retry.")
