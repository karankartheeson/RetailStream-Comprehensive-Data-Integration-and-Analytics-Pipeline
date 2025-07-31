import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, date_format, when
# Get the job parameters from arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Read CSV files directly from the specified source S3 path
df = spark.read.option("header", "true").csv(args['source_path'])
# Data transformations
# Change the data type of the 'Date' column to DateType (assuming 'dd/MM/yyyy' format)
df = df.withColumn('Date', to_date(col('Date'), 'dd/MM/yyyy'))
# Convert the date to 'YYYY-MM-DD' format
df = df.withColumn('Date', date_format(col('Date'), 'yyyy-MM-dd'))
# Convert the 'IsHoliday' column to 1 for true and 0 for false
df = df.withColumn('IsHoliday', when(col('IsHoliday') == 'True', 1).otherwise(0))
# Drop duplicates if necessary
df = df.dropDuplicates()
# Repartition the DataFrame to a single file (optional for single output CSV)
df = df.coalesce(1)
# Convert to DynamicFrame
final_data = DynamicFrame.fromDF(df, glueContext, "final_data")
# Write the transformed data to the specified S3 target path as CSV
glueContext.write_dynamic_frame.from_options(
  frame=final_data,
  connection_type="s3",
  connection_options={"path": args['target_path']},
  format="csv"
)
# Commit the Glue job
job.commit()
