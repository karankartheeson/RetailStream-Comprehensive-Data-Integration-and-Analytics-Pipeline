from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col, when, to_date

logging.basicConfig(level=logging.INFO)

try:
    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
	 .appName("CleanAndPushToHive") \
 	 .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
 	 .config("hive.metastore.uris", "thrift://localhost:9083") \
 	 .config("hive.exec.dynamic.partition", "true") \
 	 .config("hive.exec.dynamic.partition.mode", "nonstrict") \
 	 .enableHiveSupport() \
 	 .getOrCreate()


    # Load all CSVs from HDFS folder
    logging.info("Reading CSV files from HDFS...")
    df = spark.read.csv('/user/hadoop/features_data/*.csv', header=True, inferSchema=True)
    # Data cleaning
    df = df.withColumn('Date', to_date(col('Date'), 'dd/MM/yyyy'))
    df = df.withColumn('IsHoliday', when(col('IsHoliday') == 'TRUE', 1).otherwise(0))
    df_cleaned = df.dropDuplicates()
    # Optional: drop or fill NaNs if needed (adjust per data type)
    # df_cleaned = df_cleaned.na.fill(0) # Fill with 0s if needed
    # Create Hive database and write table
    spark.sql("CREATE DATABASE IF NOT EXISTS dmart")
    spark.sql("USE dmart")
    logging.info("Writing DataFrame to Hive table...")
    df_cleaned.write.mode('overwrite').saveAsTable('features_data')
    logging.info("Showing sample data from Hive table...")
    spark.sql("SELECT * FROM dmart.features_data LIMIT 10").show()

except Exception as e:
    logging.error(f"Error occurred: {e}")
finally:
    try:
        spark.stop()
    except:
        pass
