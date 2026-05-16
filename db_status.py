import os, json, boto3, psycopg2
from tabulate import tabulate

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
cur = conn.cursor()

rows = []
for t in ['customers', 'products', 'orders', 'order_items', 'inventory']:
    cur.execute(f'SELECT COUNT(*) FROM source.{t}')
    src = cur.fetchone()[0]
    cur.execute(f'SELECT COUNT(*) FROM target.{t}')
    tgt = cur.fetchone()[0]
    rows.append([t, src, tgt, src - tgt])

print(tabulate(rows, headers=['Table', 'Source', 'Target', 'Lag'], tablefmt='rounded_outline'))

print("\n--- Sample value comparison (customers) ---")
cur.execute("""
    SELECT s.id, s.name, s.status,
           t.name AS tgt_name, t.status AS tgt_status,
           CASE WHEN s.name=t.name AND s.status=t.status THEN 'MATCH' ELSE 'MISMATCH' END
    FROM source.customers s
    LEFT JOIN target.customers t ON s.id = t.id
    ORDER BY s.id LIMIT 10
""")
print(tabulate(cur.fetchall(),
               headers=['id', 'src_name', 'src_status', 'tgt_name', 'tgt_status', 'result'],
               tablefmt='rounded_outline'))

conn.close()
