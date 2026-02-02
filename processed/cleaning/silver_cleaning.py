# processed/cleaning/silver_cleaning.py
import os
import shutil
from pyspark.sql.functions import col, when, regexp_replace, current_timestamp
from pyspark.sql.types import DateType, DoubleType
from config.spark_session import get_spark
from config.path import BRONZE_DATA_PATH, SILVER_CLEAN_DATA_PATH
from utils.logger import get_logger


def parquet_has_data(spark, path):
    """Check whether a parquet path exists AND contains readable data"""
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except Exception:
        return False


def silver_cleaning(input_df=None):

    # Initialize Spark
    spark = get_spark()

    logger = get_logger(
        name="Silver Cleaning",
        log_file="silver_cleaning.log"
    )

    logger.info("Starting Silver Cleaning Process...")


    # Load Bronze Data (Snapshot OR Incremental)
    logger.info("Loading Bronze Data")

    if input_df is not None:
        bronze_df = input_df
        bronze_count = bronze_df.count()
        logger.info(f"Incremental Bronze rows received : {bronze_count}")
    else:
        if not os.path.exists(BRONZE_DATA_PATH):
            logger.error("Bronze Data not found. Silver Cleaning aborted.")
            return None

        bronze_df = spark.read.parquet(BRONZE_DATA_PATH)
        bronze_count = bronze_df.count()
        logger.info(f"Total Bronze rows detected : {bronze_count}")

    if bronze_count == 0:
        logger.warning("Bronze is empty. Exiting Silver Cleaning.")
        return

    # Read Existing Silver (safe check)
    if parquet_has_data(spark, SILVER_CLEAN_DATA_PATH):
        silver_existing_df = spark.read.parquet(SILVER_CLEAN_DATA_PATH)
        silver_existing_count = silver_existing_df.count()
        logger.info(f"Existing Silver rows detected : {silver_existing_count}")
    else:
        silver_existing_count = 0
        logger.info("No valid existing Silver data found")

    # Data Cleaning
    logger.info("Dropping duplicate Campaign_ID records")
    df = bronze_df.dropDuplicates(["Campaign_ID"])

    # 1. Handle NULL Campaign_ID
    logger.info("Dropping rows with NULL Campaign_ID")
    df = df.filter(col("Campaign_ID").isNotNull())

    # 2. Clean numeric columns
    logger.info("Cleaning numeric columns")
    df = (
        df
        .withColumn("Conversion_Rate", when(col("Conversion_Rate").isNull(), 0).otherwise(col("Conversion_Rate")))
        .withColumn(
            "Acquisition_Cost",
            when(col("Acquisition_Cost").isNull(), 0)
            .otherwise(regexp_replace(col("Acquisition_Cost"), "[$,]", "").cast(DoubleType()))
        )
        .withColumn("ROI", when(col("ROI").isNull(), 0).otherwise(col("ROI")))
        .withColumn("Clicks", when(col("Clicks").isNull(), 0).otherwise(col("Clicks")))
        .withColumn("Impressions", when(col("Impressions").isNull(), 0).otherwise(col("Impressions")))
        .withColumn("Engagement_Score", when(col("Engagement_Score").isNull(), 0).otherwise(col("Engagement_Score")))
    )

    # 3. Convert Date column
    logger.info("Converting Date column to DateType")
    df = df.withColumn("Date", col("Date").cast(DateType()))

    # Add Silver timestamp
    df = df.withColumn("silver_cleaned_timestamp", current_timestamp())

    final_count = df.count()
    logger.info(f"Final Silver row count after cleaning : {final_count}")

    if final_count == 0:
        logger.warning("No valid records after cleaning. Exiting Silver Cleaning.")
        return

    # Safe Write (TEMP → FINAL)
    temp_path = SILVER_CLEAN_DATA_PATH + "_tmp"

    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)

    logger.info("Writing Silver Data to TEMP Path")
    df.write.mode("overwrite").parquet(temp_path)

    if os.path.exists(SILVER_CLEAN_DATA_PATH):
        shutil.rmtree(SILVER_CLEAN_DATA_PATH)

    os.rename(temp_path, SILVER_CLEAN_DATA_PATH)
    logger.info("Silver Cleaning Completed Successfully")

    return df

if __name__ == "__main__":
    silver_cleaning()