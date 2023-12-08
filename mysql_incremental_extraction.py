import pymysql
import pandas as pd
import boto3,sys
from awsglue.utils import getResolvedOptions

session  = boto3.Session()

args = getResolvedOptions(sys.argv,['table_name'])

table_name = args['table_name']
date_filter = "2018-08-08"

def mysql_connect():
    conn = pymysql.connect(
        host='ecommerce-db.c2a9v1unl3lw.eu-west-1.rds.amazonaws.com',
        user='admin', 
        password = 'Udemy123',
        db='ecommerce_db'
        )
    return conn

conn = mysql_connect()

query = """select 
                *
            from 
                {}
            where 
                date(order_purchase_timestamp)='{}' 
            limit 1000
            """.format(table_name,date_filter)

df = pd.read_sql(query, conn)

df['order_approved_at'] = pd.to_datetime(df['order_approved_at'], errors='coerce')
df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'], errors='coerce')
df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')

df.to_csv("s3://nl-dwh-transactions/ecommerce_db/"+table_name+"/"+date_filter+"/data.csv",header=True)

# df.to_parquet("s3://nl-dwh-transactions/"+table_name+"/"+date_filter+"/data.parquet",engine='pyarrow')