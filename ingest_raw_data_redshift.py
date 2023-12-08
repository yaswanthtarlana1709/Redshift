import redshift_connector,sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,['table_name','file_format'])

table_name = args['table_name']
file_format = args['file_format']

date_filter = "2018-08-12"
bucket_name= "nl-rawdata"

conn = redshift_connector.connect(
     host='dwh-data-analytics.cqhdehqrcrp2.eu-west-1.redshift.amazonaws.com',
     database='dev',
     port=5439,
     user='awsuser',
     password=''
)

cursor = conn.cursor()

cursor.execute("BEGIN;")

if file_format=='csv':
    copy_qry = """
        copy transactional_layer.{} from 's3://{}/ecommerce_db/{}/{}/data.{}'
        iam_role 'arn:aws:iam::127489365181:role/redshift_copy_role'
        CSV QUOTE '\"' DELIMITER ','
        IGNOREHEADER 1
        acceptinvchars;
    """.format(table_name,bucket_name,table_name,str(date_filter),'csv')
    
elif file_format=='parquet':
    copy_qry = """
        copy transactional_layer.{} from 's3://{}/ecommerce_db/{}/{}/data.{}'
        iam_role 'arn:aws:iam::127489365181:role/redshift_copy_role'
        format parquet;
    """.format(table_name,bucket_name,table_name,str(date_filter),'parquet')

cursor.execute(copy_qry)

cursor.execute("END;")

cursor.close()
conn.close()
