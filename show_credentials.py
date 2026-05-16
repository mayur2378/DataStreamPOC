import os, json, boto3

secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1').get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
print('Username:', secret['username'])
print('Password:', secret['password'])
