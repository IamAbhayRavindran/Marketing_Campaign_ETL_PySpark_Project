#incremental/incremental_load.py
import os
from pyspark.sql.functions import col, max as spark_max
from config.spark_session import get_spark
from config.path import (
    BRONZE_DATA_PATH, SILVER_CLEAN_DATA_PATH, SILVER_FILTER_FINAL_PATH,
    GOLD_CAMPAIGN_SUMMARY, GOLD_CHANNEL_SUMMARY, GOLD_DAILY_SUMMARY
)
from utils.logger import get_logger
from ingestion.bronze_ingestion import bronze_ingestion
from processed.cleaning.silver_cleaning import silver_cleaning
from processed.filter_final.silver_filter_final import silver_filter_final
from aggregation.gold_agg import gold_aggregation


def incremental_load():
    """
    Hybrid Incremental ETL Pipeline:
    Bronze -> Incremental append
    Silver -> Incremental append
    Silver Final -> Incremental append
    Gold -> Incremental aggregation
    """

    logger = get_logger(
        name = "Incremental Load",
        log_file = "incremental_load.log"
    )

    # Initialize Spark
    spark = get_spark()
    logger.info("Starting Hybrid Incremental Load Pipeline...")


    # 1. Bronze Layer (Incremental)
    logger.info("Running Bronze Incremental Logic...")

    # Read raw CSV & add ingestion_timestamp
    df_bronze_new = bronze_ingestion().cache()

    if os.path.exists(BRONZE_DATA_PATH):
        logger.info("Existing Bronze Data found. Applying incremental filter...")

        df_existing = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .parquet(BRONZE_DATA_PATH)
        )

        last_ts = (
            df_existing
            .select(spark_max("ingestion_timestamp"))
            .collect()[0][0]
        )

        logger.info(f"Last ingestion timestamp in Bronze : {last_ts}")

        # Keep only new records
        df_bronze_new = (
            df_bronze_new
            .filter(col("ingestion_timestamp") > last_ts)
            .cache()
        )

        before = df_bronze_new.count()
        logger.info(f"New Bronze rows after timestamp filter : {before}")

        # Deduplicate by Campaign_ID
        logger.info("Removing duplicate Campaign_ID values...")

        df_bronze_new = (
            df_bronze_new
            .join(
                df_existing.select("Campaign_ID").distinct(),
                on="Campaign_ID",
                how="left_anti"
            )
            .cache()
        )

        after = df_bronze_new.count()
        logger.info(f"Bronze deduplication complete : {after}/{before} rows kept")

        if after > 0:
            df_bronze_new.count.mode("append").parquet(BRONZE_DATA_PATH)

    else:
        logger.info("No Bronze Data found. Creating initial Bronze dataset...")
        df_bronze_new.write.mode("overwrite").parquet(BRONZE_DATA_PATH)
        after = df_bronze_new.count()

    logger.info(f"New Bronze rows added : {after}")

    if after == 0:
        logger.info("No new Bronze Data. Pipeline stopped.")
        return


    # 2. Silver Cleaned (Incremental)
    logger.info("Running Silver Cleaned Incremental Logic...")

    df_silver_cleaned = silver_cleaning(df_bronze_new).cache()
    cleaned_count = df_silver_cleaned.count()

    logger.info(f"New Silver Cleaned rows : {cleaned_count}")

    if cleaned_count == 0:
        logger.info("No new Silver Cleaned Data. Pipeline stopped.")
        return

    silver_clean_write_mode = (
        "append"
        if os.path.exists(SILVER_CLEAN_DATA_PATH)
        else "overwrite"
    )

    df_silver_cleaned.write.mode(silver_clean_write_mode).parquet(SILVER_CLEAN_DATA_PATH)


    # 3 Silver Filter Final (Incremental)
    logger.info("Running Silver Filter Final Incremental Logic...")

    #  Important: silver_filter_final must RETURN a DataFrame
    df_silver_filtered = silver_filter_final(df_silver_cleaned).cache()
    filtered_count = df_silver_filtered.count()

    logger.info(f"New Silver Filtered rows : {filtered_count}")

    if filtered_count == 0:
        logger.info("No new Silver Filtered Data. Pipeline stopped.")
        return

    silver_filter_write_mode = (
        "append"
        if os.path.exists(SILVER_FILTER_FINAL_PATH)
        else "overwrite"
    )

    df_silver_filtered.write.mode(silver_filter_write_mode).parquet(SILVER_FILTER_FINAL_PATH)


    # 4. Gold Aggregation (Incremental)
    logger.info("Running Gold Incremental Aggregations...")

    df_campaign, df_channel, df_daily = gold_aggregation(df_silver_filtered)

    logger.info(f"New Campaign Summary Rows : {df_campaign.count()}")
    logger.info(f"New Channel Summary Rows : {df_channel.count()}")
    logger.info(f"New Daily Summary Rows : {df_daily.count()}")

    logger.info("Hybrid Incremental Load Completed Successfully...")


if __name__ == "__main__":
    incremental_load()

