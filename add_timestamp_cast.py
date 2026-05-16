import os, json, boto3, psycopg2

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb', user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE OR REPLACE FUNCTION ms_epoch_to_timestamp(ms bigint)
RETURNS timestamp without time zone AS $$
SELECT to_timestamp(ms / 1000.0)::timestamp without time zone;
$$ LANGUAGE SQL IMMUTABLE STRICT;
""")
print("Function created.")

cur.execute("""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_cast
        WHERE castsource = 'bigint'::regtype
        AND   casttarget = 'timestamp without time zone'::regtype
    ) THEN
        CREATE CAST (bigint AS timestamp without time zone)
            WITH FUNCTION ms_epoch_to_timestamp(bigint) AS ASSIGNMENT;
        RAISE NOTICE 'Cast created.';
    ELSE
        RAISE NOTICE 'Cast already exists, skipping.';
    END IF;
END $$;
""")
print("Cast in place.")

conn.close()
print("Done. Now recreate the sink connector so it retries from offset 0.")
