# aggregation/gold_agg.py
import os
import shutil
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, when,
    current_timestamp, year, month, to_date, coalesce
)
from config.path import SILVER_FILTER_FINAL_PATH, GOLD_BASE_PATH
from config.spark_session import get_spark
from utils.logger import get_logger


def safe_div(numerator, denominator):
    """Prevent divide-by-zero"""
    return when(denominator != 0, numerator / denominator).otherwise(0)


def gold_aggregation(preserve_invalid=False):


    # Initialize Spark
    spark = get_spark()
    spark.catalog.clearCache()

    logger = get_logger(
        name="Gold Aggregation",
        log_file="gold_aggregation.log",
    )

    write_mode = "overwrite"

    logger.info("Starting Gold Aggregation Process...")

    # Read Silver Filter Final
    if not os.path.exists(SILVER_FILTER_FINAL_PATH):
        logger.warning("Silver Filter Final not found → Gold not generated")
        return

    df_silver = spark.read.parquet(SILVER_FILTER_FINAL_PATH)
    total_count = df_silver.count()
    logger.info(f"Total Silver Filter Final records: {total_count}")

    if total_count == 0:
        logger.warning("Silver Filter Final is empty → Gold not generated")
        return

    # Identify nulls (Null Audit Before)
    null_campaign = df_silver.filter(col("Campaign_ID").isNull()).count()
    null_date_raw = df_silver.filter(col("Date").isNull()).count()

    logger.info(
        f"Initial NULL check -> Campaign_ID : {null_campaign}, Date : {null_date_raw}"
    )

    # Clean Data
    numeric_cols = [
        "Impressions", "Clicks", "Engagement_Score",
        "Acquisition_Cost", "Conversion_Rate", "ROI"
    ]

    string_cols = [
        "Company", "Campaign_Type", "Target_Audience",
        "Duration", "Channel_Used", "Location",
        "Language", "Customer_Segment"
    ]

    df = (
        df_silver
        .fillna({c: 0 for c in numeric_cols})
        .fillna({c: "Unknown" for c in string_cols})
    )

    # Safe Date Parsing (Critical Fix)
    df = df.withColumn(
        "Date",
        coalesce(
            to_date(col("Date"), "yyyy-MM-dd"),
            to_date(col("Date"), "dd-MM-yyyy"),
            to_date(col("Date"), "MM/dd/yyyy")
        )
    )

    # Null Audit (After Parse)
    invalid_campaign = df.filter(col("Campaign_ID").isNull()).count()
    invalid_date = df.filter(col("Date").isNull()).count()

    logger.info(
        f"After Date parsing → Invalid Campaign_ID: {invalid_campaign}, Invalid Date: {invalid_date}"
    )


    # Datasets for Aggregation

    # Campaign & Channel → Date NOT required
    df_campaign_channel = df.filter(col("Campaign_ID").isNotNull())

    # Daily → Date REQUIRED
    df_daily = df.filter(
        col("Campaign_ID").isNotNull() &
        col("Date").isNotNull()
    )

    campaign_channel_count = df_campaign_channel.count()
    daily_valid_count = df_daily.count()

    logger.info(
        f"Records used -> Campaign/Channel: {campaign_channel_count}, Daily(valid date): {daily_valid_count}"
    )

    # Add Date Parts (Only Where Needed)
    df_daily = (
        df_daily
        .withColumn("year", year(col("Date")))
        .withColumn("month", month(col("Date")))
    )



    # 1. Campaign Summary
    logger.info("Creating Campaign Summary")

    campaign_summary = (
        df_campaign_channel
        .groupBy("Campaign_ID", "Campaign_Type", "Company")
        .agg(
            _sum("Impressions").alias("Total_Impressions"),
            _sum("Clicks").alias("Total_Clicks"),
            _sum("Engagement_Score").alias("Total_Engagement_Score"),
            _sum("Acquisition_Cost").alias("Total_Acquisition_Cost"),
            safe_div(
                _sum(col("Conversion_Rate") * col("Impressions")),
                _sum("Impressions")
            ).alias("Avg_Conversion_Rate"),
            safe_div(_sum("ROI"), count("Campaign_ID")).alias("Avg_ROI")
        )
        .withColumn("aggregation_timestamp", current_timestamp())
    )

    campaign_path = os.path.join(GOLD_BASE_PATH, "campaign_summary")
    campaign_summary.write.mode(write_mode).parquet(campaign_path)
    logger.info(f"Campaign Summary rows: {campaign_summary.count()}")



    # 2. Channel Summary
    logger.info("Creating Channel Summary")

    channel_summary = (
        df_campaign_channel
        .groupBy("Channel_Used")
        .agg(
            _sum("Impressions").alias("Total_Impressions"),
            _sum("Clicks").alias("Total_Clicks"),
            _sum("Engagement_Score").alias("Total_Engagement_Score"),
            _sum("Acquisition_Cost").alias("Total_Acquisition_Cost"),
            avg("Conversion_Rate").alias("Avg_Conversion_Rate"),
            avg("ROI").alias("Avg_ROI")
        )
        .withColumn("aggregation_timestamp", current_timestamp())
    )

    channel_path = os.path.join(GOLD_BASE_PATH, "channel_summary")
    channel_summary.write.mode(write_mode).parquet(channel_path)
    logger.info(f"Channel Summary rows: {channel_summary.count()}")



    # 3. Daily Summary
    logger.info("Creating Daily Summary")

    daily_summary = (
        df_daily
        .groupBy("Date")
        .agg(
            _sum("Impressions").alias("Total_Impressions"),
            _sum("Clicks").alias("Total_Clicks"),
            _sum("Engagement_Score").alias("Total_Engagement_Score"),
            _sum("Acquisition_Cost").alias("Total_Acquisition_Cost"),
            avg("Conversion_Rate").alias("Avg_Conversion_Rate"),
            avg("ROI").alias("Avg_ROI")
        )
        .withColumn("aggregation_timestamp", current_timestamp())
    )

    daily_path = os.path.join(GOLD_BASE_PATH, "daily_summary")
    daily_summary.write.mode(write_mode).parquet(daily_path)
    logger.info(f"Daily Summary rows: {daily_summary.count()}")

    logger.info("Gold Aggregation completed successfully...")
    return campaign_summary, channel_summary, daily_summary


if __name__ == "__main__":
    # Set preserve_invalid=True if you want all rows to be kept
    gold_aggregation(preserve_invalid=False)

