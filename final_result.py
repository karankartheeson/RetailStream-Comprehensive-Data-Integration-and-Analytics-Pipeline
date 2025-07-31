from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    month, year, avg, sum, first, col, coalesce, lit,
    count, min, when, expr
)
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RetailAnalytics")

try:
    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
        .appName("RetailAnalytics") \
        .enableHiveSupport() \
        .getOrCreate()

    # Load data from Hive
    logger.info("Loading data from Hive tables...")
    sales_data_df = spark.sql("SELECT * FROM dmart.sales_data")
    store_data_df = spark.sql("SELECT * FROM dmart.store_data")
    features_data_df = spark.sql("SELECT * FROM dmart.features_data")

    # Cache reused DataFrames
    sales_data_df.cache()
    features_data_df.cache()
    store_data_df.cache()

    # ==============================
    # 1. Customer Visit Analysis: Type B Stores in April
    # ==============================
    logger.info("Running Customer Visit Analysis for Type B stores in April...")
    type_b_stores_df = store_data_df.filter(col("type") == 'B')
    sales_with_cpi_df = sales_data_df.join(features_data_df, ["store", "date"])
    april_sales_df = sales_with_cpi_df.join(type_b_stores_df, "store") \
        .filter(month(col("date")) == 4)

    average_visits_df = april_sales_df.withColumn(
        "customer_visits",
        when(col("cpi").isNull() | (col("cpi") == 0), 0)
        .otherwise(col("weekly_sales") / col("cpi"))
    )
    average_visits_result = average_visits_df.groupBy("store") \
        .agg(avg("customer_visits").alias("avg_customer_visits")) \
        .orderBy(col("avg_customer_visits").desc())

    average_visits_result.show()

    # ==============================
    # 2. Holiday Sales Analysis
    # ==============================
    logger.info("Running Holiday Sales Analysis...")
    holiday_sales_df = sales_data_df.filter(col("isholiday") == 1)
    avg_holiday_sales_df = holiday_sales_df.groupBy("store") \
        .agg(avg("weekly_sales").alias("avg_sales")) \
        .orderBy(col("avg_sales").desc())

    avg_holiday_sales_df.show()

    # ==============================
    # 3. Leap Year Sales Analysis
    # ==============================
    logger.info("Running Leap Year Sales Analysis...")
    leap_year_sales_df = sales_data_df.filter(
        expr("((year(date) % 4 = 0 AND year(date) % 100 != 0) OR year(date) % 400 = 0)")
    )
    leap_year_sales_result = leap_year_sales_df.groupBy("store") \
        .agg(sum("weekly_sales").alias("total_sales")) \
        .orderBy(col("total_sales").asc())

    leap_year_sales_result.show()

    # ==============================
    # 4. Sales Prediction with Unemployment Factor
    # ==============================
    logger.info("Running Sales Prediction with Unemployment Factor...")
    high_unemployment_df = features_data_df.filter(col("unemployment").cast("double") >= 8.0)
    sales_with_unemployment_df = sales_data_df.join(high_unemployment_df, ["store", "date"], "inner")

    predicted_sales_df = sales_with_unemployment_df.groupBy("store", "dept", "unemployment") \
        .agg(avg("weekly_sales").alias("avg_weekly_sales")) \
        .orderBy(col("unemployment").desc(), "store", "dept", "avg_weekly_sales")

    predicted_sales_df.select("unemployment", "store", "dept", "avg_weekly_sales").show()

    # ==============================
    # 5. Monthly Sales Aggregation
    # ==============================
    logger.info("Running Monthly Sales Aggregation...")
    monthly_sales_df = sales_data_df.groupBy("dept", month(col("date")).alias("month")) \
        .agg(sum("weekly_sales").alias("total_sales")) \
        .orderBy("dept", "month")

    monthly_sales_df.show()

    # ==============================
    # 6. Weekly High Sales Store Identification
    # ==============================
    logger.info("Running Weekly High Sales Store Identification...")
    weekly_sales_df = sales_data_df.groupBy("store", "date") \
        .agg(sum("weekly_sales").alias("total_sales"))
    windowed_sales_df = weekly_sales_df.orderBy("date", col("total_sales").desc())
    weekly_high_sales_df = windowed_sales_df.groupBy("date") \
        .agg(first("store").alias("top_store"))
    top_store_count_df = weekly_high_sales_df.groupBy("top_store") \
        .agg(count("date").alias("weeks_as_top_store")) \
        .orderBy(col("weeks_as_top_store").desc())

    top_store_count_df.show()

    # ==============================
    # 7. Department Performance Analysis
    # ==============================
    logger.info("Running Department Performance Analysis...")
    dept_performance_df = sales_data_df.groupBy("store", "dept") \
        .agg(sum("weekly_sales").alias("total_sales")) \
        .orderBy(col("total_sales").desc())

    dept_performance_df.show()

    # ==============================
    # 8. Fuel Price Analysis
    # ==============================
    logger.info("Running Fuel Price Analysis...")
    min_fuel_price_df = features_data_df.groupBy("store", "date") \
        .agg(min("fuel_price").alias("min_fuel_price")) \
        .orderBy(col("min_fuel_price").asc())

    min_fuel_price_df.show()

    # ==============================
    # 9. Yearly Store Performance
    # ==============================
    logger.info("Running Yearly Store Performance Analysis...")
    yearly_performance_df = sales_data_df.groupBy("store", year(col("date")).alias("year")) \
        .agg(sum("weekly_sales").alias("total_sales")) \
        .orderBy("year", col("total_sales").desc())

    yearly_performance_df.show()

    # ==============================
    # 10. Weekly Performance Analysis with Offers
    # ==============================
    logger.info("Running Weekly Performance Analysis with/without Offers...")
    features_data_df = features_data_df.withColumn(
        "total_markdown",
        coalesce(col("markdown1").cast("double"), lit(0.0)) +
        coalesce(col("markdown2").cast("double"), lit(0.0)) +
        coalesce(col("markdown3").cast("double"), lit(0.0)) +
        coalesce(col("markdown4").cast("double"), lit(0.0)) +
        coalesce(col("markdown5").cast("double"), lit(0.0))
    )

    sales_with_features_df = sales_data_df.join(features_data_df, ["store", "date"], "inner")

    # Sales with offers
    sales_with_offers_df = sales_with_features_df.filter(col("total_markdown") > 0)
    weekly_sales_with_offers_df = sales_with_offers_df.groupBy("store", "date").agg(
        sum("weekly_sales").alias("total_sales_with_offers"),
        first("total_markdown").alias("total_markdown")
    ).orderBy(col("total_sales_with_offers").desc())

    print("Weekly Sales with Offers (High to Low):")
    weekly_sales_with_offers_df.show()

    # Sales without offers
    sales_without_offers_df = sales_with_features_df.filter(col("total_markdown") == 0)
    weekly_sales_without_offers_df = sales_without_offers_df.groupBy("store", "date").agg(
        sum("weekly_sales").alias("total_sales_without_offers"),
        first("total_markdown").alias("total_markdown")
    ).orderBy(col("total_sales_without_offers").desc())

    print("Weekly Sales without Offers (High to Low):")
    weekly_sales_without_offers_df.show()

except Exception as e:
    logger.error(f"Error occurred during processing: {e}")

finally:
    logger.info("Stopping Spark session.")
    try:
        spark.stop()
    except:
        pass
