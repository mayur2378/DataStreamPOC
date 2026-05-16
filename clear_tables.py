import os, json, boto3, psycopg2

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()

cur.execute("SET search_path = source")
for t in ['order_items', 'inventory', 'orders', 'products', 'customers']:
    cur.execute(f"DELETE FROM {t}")
    print(f"Deleted source.{t}")
for t in ['customers', 'products', 'orders', 'order_items', 'inventory']:
    cur.execute(f"ALTER SEQUENCE {t}_id_seq RESTART WITH 1")

cur.execute("TRUNCATE target.order_items, target.inventory, target.orders, target.products, target.customers RESTART IDENTITY CASCADE")
print("Truncated all target tables")

conn.close()
print("Done. Wait ~15s then re-seed.")
