import pymysql
import pandas as pd
import boto3,sys,json
from awsglue.utils import getResolvedOptions

session  = boto3.Session()

region = 'eu-west-1'
secrets_client = session.client('secretsmanager', region_name=region)
secret_name = 'ecommerce_db'
response = secrets_client.get_secret_value(SecretId=secret_name)
secret = json.loads(response['SecretString'])

args = getResolvedOptions(sys.argv,['table_name'])

table_name = args['table_name']

def mysql_connect():
    conn = pymysql.connect(
        host=secret['host'],
        user=secret['username'], 
        password = secret['password'],
        db=secret['db_name']
        )
    return conn

conn = mysql_connect()

query = """select * from products"""

df = pd.read_sql(query, conn)

# df.to_csv("s3://nl-dwh-transactions/ecommerce_db/"+table_name+"/data.csv",header=True)

df.to_parquet("s3://nl-rawdata/"+table_name+"/data.parquet",engine='pyarrow')