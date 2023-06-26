import psycopg2
import csv
import boto3
import configparser

# load the postgres_config values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config","password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

# connect to the database
conn = psycopg2.connect(
 "dbname=" + dbname + " user=" + user + " password=" + password + " host=" + host, port = port)

# extract the data from the database
m_query = "SELECT * FROM Orders;"
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

# write the data to a csv file
with open(local_filename, 'w') as fp:
 csv_w = csv.writer(fp, delimiter='|')
 csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get(
 "aws_boto_credentials",
 "access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name")

s3 = boto3.client(
 's3',
 aws_access_key_id = access_key,
 aws_secret_access_key = secret_key)

s3_file = local_filename

s3.upload_file(
 local_filename,
 bucket_name,
 s3_file)