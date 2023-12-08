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

args = getResolvedOptions(sys.argv,['table_name','date_filter'])

table_name = args['table_name']
date_filter = args['date_filter']

def mysql_connect():
    conn = pymysql.connect(
        host=secret['host'],
        user=secret['username'], 
        password = secret['password'],
        db=secret['db_name']
        )
    return conn

conn = mysql_connect()


if table_name=='orders': 
    query = """select 
                    *
                from 
                    {}
                where 
                    date(order_purchase_timestamp)='{}' 
                """.format(table_name,date_filter)
    
elif table_name=='order_reviews':
    query = """select 
                    *
                from 
                    {}
                where 
                    date(review_creation_date)='{}' 
                """.format(table_name,date_filter)
    
elif table_name=='order_items':
    query = """select 
                    *
                from 
                    {}
                where 
                    date(order_purchase_timestamp)='{}' 
                """.format(table_name,date_filter)

df = pd.read_sql(query, conn)

if table_name=='orders': 
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'], errors='coerce')
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'], errors='coerce')
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')

elif table_name=='order_reviews':
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')

elif table_name=='order_items':
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')

df.to_csv("s3://nl-dwh-transactions/ecommerce_db/"+table_name+"/"+date_filter+"/data.csv",header=True)

# df.to_parquet("s3://nl-dwh-transactions/"+table_name+"/"+date_filter+"/data.parquet",engine='pyarrow')