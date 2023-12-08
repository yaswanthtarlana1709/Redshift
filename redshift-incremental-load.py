import redshift_connector, sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['table_name', 'file_format', 'primary_key'])

table_name = args['table_name']
primary_key = args['primary_key']
file_format = args['file_format']
date_filter = "2018-08-12"
bucket_name = "nl-rawdata"

conn = redshift_connector.connect(
    host='your_host',
    database='dev',
    port=5439,
    user='awsuser',
    password=''
)

cursor = conn.cursor()

# Begin the transaction
cursor.execute("BEGIN;")

# Copy data from S3 to staging table
if file_format == 'csv':
    copy_qry = """
        copy transactional_layer_staging.{} from 's3://{}/ecommerce_db/{}/{}/data.{}'
        iam_role ''
        CSV QUOTE '\"' DELIMITER ','
        IGNOREHEADER 1
        acceptinvchars;
    """.format(table_name, bucket_name, table_name, date_filter, 'csv')
elif file_format == 'parquet':
    copy_qry = """
        copy transactional_layer_staging.{} from 's3://{}/ecommerce_db/{}/{}/data.{}'
        iam_role ''
        format parquet;
    """.format(table_name, bucket_name, table_name, date_filter, 'parquet')
cursor.execute(copy_qry)

# Delete records from target table that are about to be refreshed
delete_qry = """
    delete from transactional_layer.{} 
    using transactional_layer_staging.{} 
    where 
        transactional_layer.{}.{} = transactional_layer_staging.{}.{};
""".format(table_name, table_name, table_name, primary_key, table_name, primary_key)
cursor.execute(delete_qry)

# Insert data from staging table to target table
insert_qry = """insert into transactional_layer.{} select * from transactional_layer_staging.{};""".format(table_name, table_name)
cursor.execute(insert_qry)

# Clean up staging table
truncate_staging_qry = """truncate table transactional_layer_staging.{};""".format(table_name)
cursor.execute(truncate_staging_qry)

# Commit the transaction
cursor.execute("COMMIT;")

cursor.close()
conn.close()
