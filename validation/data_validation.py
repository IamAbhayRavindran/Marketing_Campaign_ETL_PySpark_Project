#validation/data_validation.py
import os
from pyspark.sql.functions import col, sum as spark_sum, when
from config.spark_session import get_spark
from config.path import (
    SILVER_FILTER_FINAL_PATH,
    GOLD_CAMPAIGN_SUMMARY,
    GOLD_CHANNEL_SUMMARY,
    GOLD_DAILY_SUMMARY
)
from utils.logger import get_logger


def validate_data():
    logger = get_logger(
        name = "Data Validation",
        log_file = "validation.log"
    )

    # Initialize Spark
    spark = get_spark()


    # Load Silver Final Layer
    logger.info("Loading Silver Final Data...")
    df_silver = spark.read.parquet(SILVER_FILTER_FINAL_PATH)
    df_silver.cache()

    silver_count = df_silver.count()
    logger.info(f"Silver Final Count : {silver_count}")

    logger.info("Silver layer schema:")
    df_silver.printSchema()

    if silver_count == 0:
        logger.error("Silver Final is empty. Validation aborted.")
        df_silver.unpersist()
        return


    # Load Gold Aggregated Layers
    logger.info("Load Gold Aggregation Outputs...")

    if not (
        os.path.exists(GOLD_CAMPAIGN_SUMMARY)
        and os.path.exists(GOLD_CHANNEL_SUMMARY)
        and os.path.exists(GOLD_DAILY_SUMMARY)
    ):
        logger.error("One or more Gold output path are missing. Skipping Gold validation.")
        df_silver.unpersist()
        return

    df_gold_campaigns = spark.read.parquet(GOLD_CAMPAIGN_SUMMARY)
    df_gold_channels = spark.read.parquet(GOLD_CHANNEL_SUMMARY)
    df_gold_daily = spark.read.parquet(GOLD_DAILY_SUMMARY)

    logger.info(f"Gold Campaign Summary Count : {df_gold_campaigns.count()}")
    logger.info(f"Gold Channel Summary Count : {df_gold_channels.count()}")
    logger.info(f"Gold Daily Summary Count : {df_gold_daily.count()}")


    # 1 Critical Column Null Check (Silver Layer)
    critical_cols = ["Campaign_ID", "Company", "Channel_Used"]

    logger.info("Checking missing values in critical column (Silver layer)...")

    for c in critical_cols:
        null_count = df_silver.filter(col(c).isNull()).count()
        if null_count > 0:
            logger.error(f"Column '{c}' has {null_count} missing NULL values.")
        else:
            logger.info(f"Column '{c}' has no missing values.")


    # 2 Negative Value Validation
    numeric_cols = [
        "Clicks", "Impressions", "Conversion_Rate",
        "Acquisition_Cost", "ROI", "Engagement_Score"
    ]

    logger.info("Checking for Negative Values in Numeric Columns...")

    for c in numeric_cols:
        negative_count = df_silver.filter(col(c) < 0).count()
        if negative_count > 0:
            logger.error(f"Column '{c}' contains {negative_count} negative values.")
        else:
            logger.info(f"Column '{c}' has no negative values.")


    # 3 CTR & Conversion Rate Check
    logger.info("Validating CTR & Conversion Rate ranges (0 to 1)...")

    # CTR = Clicks / Impressions
    df_silver = df_silver.withColumn(
        "CTR",
        when(col("Impressions") > 0, col("Clicks") / col("Impressions")).otherwise(0)
    )

    invalid_ctr = df_silver.filter(
        (col("CTR") < 0) | (col("CTR") > 1)
    ).count()

    invalid_conv = df_silver.filter(
        (col("Conversion_Rate") < 0) | (col("Conversion_Rate") > 1)
    ).count()

    if invalid_ctr > 0:
        logger.error(f"{invalid_ctr} rows have invalid CTR values.")
    else:
        logger.info("CTR range validation passed.")

    if invalid_ctr > 0:
        logger.error(f"{invalid_conv} rows have invalid Conversion Rate values.")
    else:
        logger.info("Conversion Rate range validation passed.")


    # 4 Silver vs Gold Total Consistency Check
    logger.info("Comparing Silver & Gold aggregation totals...")

    silver_totals = df_silver.agg(
        spark_sum("Clicks").alias("silver_clicks"),
        spark_sum("Impressions").alias("silver_impressions")
    ).collect()[0]

    gold_totals = df_gold_daily.agg(
        spark_sum("Total_Clicks").alias("gold_clicks"),
        spark_sum("Total_Impressions").alias("gold_impressions")
    ).collect()[0]

    if silver_totals["silver_clicks"] == gold_totals["gold_clicks"]:
        logger.info("Total Clicks match between Silver & Gold layers.")
    else:
        logger.error(
            f"Click mismatch : Silver={silver_totals['silver_clicks']}, "
            f"Gold={gold_totals['gold_clicks']}"
        )

    if silver_totals["silver_impressions"] == gold_totals["gold_impressions"]:
        logger.info("Total Impressions match between Silver & Gold layers.")
    else:
        logger.error(
            f"Impressions mismatch : Silver={silver_totals['silver_impressions']}, "
            f"Gold={gold_totals['gold_impressions']}"
        )


    # Cleanup
    df_silver.unpersist()
    logger.error("Data Validation Process Completed Successfully...")


if __name__ == "__main__":
    validate_data()


