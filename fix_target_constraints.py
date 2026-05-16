import os, json, boto3, psycopg2

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()

tables = ['customers', 'products', 'orders', 'order_items', 'inventory']

for t in tables:
    cur.execute(f"""
        SELECT constraint_name FROM information_schema.table_constraints
        WHERE table_schema = 'target' AND table_name = '{t}'
        AND constraint_type = 'PRIMARY KEY'
    """)
    if cur.fetchone():
        print(f"SKIP: target.{t} already has a PRIMARY KEY")
    else:
        cur.execute(f"ALTER TABLE target.{t} ADD PRIMARY KEY (id)")
        print(f"OK:   target.{t} PRIMARY KEY (id) added")

conn.close()
print("\nDone. Recreate the sink connector.")
