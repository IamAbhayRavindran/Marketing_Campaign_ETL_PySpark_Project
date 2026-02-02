# processed/filter_final/silver_filter_final.py
import os
import shutil
from pyspark.sql.functions import col, current_timestamp
from config.spark_session import get_spark
from config.path import SILVER_CLEAN_DATA_PATH, SILVER_FILTER_FINAL_PATH
from utils.logger import get_logger


def parquet_exists(path):
    return os.path.exists(path)


def valid_parquet_data(path):
    if not os.path.exists(path):
        return False
    return any(f.endswith(".parquet") for f in os.listdir(path))


def silver_filter_final(input_df=None):

    spark = get_spark()
    spark.catalog.clearCache()

    logger = get_logger(
        name="Silver Filter Final",
        log_file="silver_filter_final.log"
    )

    logger.info("Starting Silver Filter Final Process...")


    # 1. Read Silver Clean (Snapshot OR Incremental)
    if input_df is not None:
        df_cleaned = input_df
        input_count = df_cleaned.count()
        logger.info(f"Incremental Silver Clean rows received : {input_count}")
    else:
        if not valid_parquet_data(SILVER_CLEAN_DATA_PATH):
            logger.warning("Silver Clean parquet not found → Nothing to process")
            return None

        df_cleaned = spark.read.parquet(SILVER_CLEAN_DATA_PATH)
        input_count = df_cleaned.count()
        logger.info(f"Input Silver Clean records : {input_count}")

    if input_count == 0:
        logger.warning("Silver Clean is empty → Nothing to process")
        return

    # 2. Business Filtering
    valid_channels = [
        "YouTube", "Email", "Instagram",
        "Website", "Google Ads", "Facebook"
    ]

    df_filtered = df_cleaned.filter(
        (col("Impressions") > 0) &
        (col("Acquisition_Cost") >= 0) &
        (col("Channel_Used").isin(valid_channels)) &
        ((col("Clicks") > 0) | (col("Conversion_Rate") > 0))
    )

    filtered_count = df_filtered.count()
    logger.info(f"Records after filtering : {filtered_count}")

    if filtered_count == 0:
        logger.warning("No records passed business filters")
        return

    df_filtered = df_filtered.withColumn(
        "filter_timestamp", current_timestamp()
    )

    # 3. Incremental logic
    if valid_parquet_data(SILVER_FILTER_FINAL_PATH):
        logger.info("Existing Silver Filter Final detected → Incremental load")

        df_existing = spark.read.parquet(SILVER_FILTER_FINAL_PATH)

        df_new = df_filtered.join(
            df_existing.select("Campaign_ID"),
            on="Campaign_ID",
            how="left_anti"
        )

        new_count = df_new.count()
        logger.info(f"New records to insert : {new_count}")

        if new_count == 0:
            logger.info("No new records → Silver Filter Final unchanged")
            return

        df_final = df_existing.unionByName(df_new)
    else:
        logger.info("First Silver Filter Final load")
        df_final = df_filtered

    df_final = df_final.dropDuplicates(["Campaign_ID"])


    # 4. Safe Write (Temp Path)
    temp_path = SILVER_FILTER_FINAL_PATH + "_tmp"

    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)

    logger.info("Writing Silver Filter Final to temp path")
    df_final.write.mode("overwrite").parquet(temp_path)

    if os.path.exists(SILVER_FILTER_FINAL_PATH):
        shutil.rmtree(SILVER_FILTER_FINAL_PATH)

    os.rename(temp_path, SILVER_FILTER_FINAL_PATH)

    logger.info("Silver Filter Final completed successfully")

    return df_new if 'df_new' in locals() else df_final


if __name__ == "__main__":
    silver_filter_final()

