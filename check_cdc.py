import os, json, boto3, psycopg2
secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=secret.get('host', os.environ.get('DB_HOST')), dbname='pocdb', user=secret['username'], password=secret['password'])
cur = conn.cursor()
cur.execute('SHOW wal_level'); print('wal_level:', cur.fetchone())
cur.execute('SELECT pubname FROM pg_publication'); print('publications:', cur.fetchall())
cur.execute("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname='poc_publication'"); print('pub tables:', cur.fetchall())
cur.execute('SELECT slot_name, active FROM pg_replication_slots'); print('slots:', cur.fetchall())
conn.close()
