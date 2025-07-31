import json
import boto3
import pymysql
import csv
import os
def lambda_handler(event, context):
  s3_path = event.get('s3_path')
  # Extract bucket and key
  bucket = s3_path.replace("s3://", "").split('/')[0]
  prefix = '/'.join(s3_path.replace("s3://", "").split('/')[1:])
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
  for obj in response.get('Contents', []):
      key = obj['Key']
      if key.endswith('.csv'):
          csv_file = s3.get_object(Bucket=bucket, Key=key)
  	  lines = csv_file['Body'].read().decode('utf-8').splitlines()
  	  reader = csv.reader(lines)
  	  headers = next(reader) # skip header
  	  connection = pymysql.connect(
  	  	host=os.environ['RDS_HOST'],
  	  	user=os.environ['RDS_USER'],
  	  	password=os.environ['RDS_PASS'],
  	  	db=os.environ['RDS_DB'],
  	  	port=3306
  	  )
  	  cursor = connection.cursor()
  	  for row in reader:
  	  	# Customize this based on your table structure
  	  	cursor.execute(
  	  	"INSERT INTO sales_data (Store, Dept, Date, Weekly_Sales, IsHoliday) VALUES (%s, %s, %s, %s, %s)",
  	  	row
  	  	)
  	  connection.commit()
  	  cursor.close()
  	  connection.close()
  return {
  'statusCode': 200,
  'body': json.dumps('Data inserted into RDS')
  }

