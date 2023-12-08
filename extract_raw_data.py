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

args = getResolvedOptions(sys.argv,['table_name','file_format'])

table_name = args['table_name']
file_format = args['file_format']

date_filter = "2018-08-12"

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
    df['review_score'] = df['review_score'].astype(str)
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')

elif table_name=='order_items':
    df['order_item_id'] = df['order_item_id'].astype(int)
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')

if file_format=='csv':
    df.to_csv("s3://nl-rawdata/ecommerce_db/"+table_name+"/"+date_filter+"/data.csv",header=True)

elif file_format=='parquet':
    df.to_parquet("s3://nl-rawdata/ecommerce_db/"+table_name+"/"+date_filter+"/data.parquet",engine='pyarrow')