# transformations/sql_metrics.py
import os
import shutil
from pathlib import Path
from pyspark.sql.functions import current_timestamp, col
from config.path import SILVER_FILTER_FINAL_PATH, SQL_METRICS_PATH
from config.spark_session import get_spark
from utils.logger import get_logger
from pathlib import Path


def valid_parquet_data(path):
    if not os.path.exists(path):
        return False
    return any(f.endswith(".parquet") for f in os.listdir(path))


def sql_metrics():

    logger = get_logger(
        name="SQL Metrics",
        log_file="sql_metrics.log"
    )


    # MODE
    write_mode = "overwrite"

    # Initialize Spark
    spark = get_spark()
    spark.catalog.clearCache()

    logger.info("Spark Session Initialized for SQL Metrics")


    # Load Silver Filter Final parquet
    if not Path(SILVER_FILTER_FINAL_PATH).exists():
        logger.warning(
            "Silver Filter Final path does not exist → SQL Metrics not generated"
        )
        return

    df = spark.read.parquet(SILVER_FILTER_FINAL_PATH)
    silver_count = df.count()
    logger.info(f"Silver records received : {silver_count}")

    if silver_count == 0:
        logger.warning("No Silver Data available for SQL Metrics")
        return


    # Incremental INPUT logging
    previous_silver_count = 0
    if valid_parquet_data(SQL_METRICS_PATH):
        try:
            previous_silver_count = spark.read.parquet(
                SILVER_FILTER_FINAL_PATH
            ).count()
        except Exception:
             previous_silver_count = 0

    new_silver_rows = silver_count - previous_silver_count

    if new_silver_rows > 0:
        logger.info(
            f"New Silver rows detected for SQL Metrics processing : {new_silver_rows}"
        )
    else:
        logger.info("No new Silver rows detected since last SQL Metrics run")


    # Add metrics timestamp
    df = df.withColumn("metrics_timestamp", current_timestamp())
    logger.info("Added metrics_timestamp column")

    #Basic Dataset Information
    logger.info(f"Silver Data loaded...")
    logger.info(f"Total Records : {silver_count}")
    logger.info(f"Columns count : {len(df.columns)}")
    logger.info(f"Columns Name : {df.columns}")

    logger.info("silver_filter_final Data Sample :")
    sample_data = df.limit(5).toJSON().collect()
    for row in sample_data:
        logger.info(row)


    # Using SparkSQL, compute per campaign :

    # Create Temporary View
    logger.info("Using SparkSQL, compute per campaign :")
    df.createOrReplaceTempView("SILVER_FILTER_FINAL")
    logger.info("Temporary View 'SILVER_FILTER_FINAL' Created Successfully...")


    #SQL METRICS QUERY

    # Calculate Click Through Rate (CTR)

    # Conversion Rate Performance (CRP)

    # High Performance → Conversion_Rate > 0.10
    # Medium Performance → Conversion_Rate between 0.05 and 0.10
    # Low Performance → Conversion_Rate < 0.05

    # Calculate Return On Investment (ROI)
    # Make it complex with outlier handling + risk levels


    logger.info("SQL Metrics Started...")

    sql_query = """
        WITH base as (
            SELECT 
                Campaign_Type,
                Clicks,
                Impressions,
                Conversion_Rate,
                ROI,
                metrics_timestamp
            FROM SILVER_FILTER_FINAL
        ),
        
        
        CTR_Calc AS (
            SELECT
                *,
                CASE
                    WHEN Impressions = 0 OR Impressions IS NULL THEN 0
                    ELSE (Clicks / Impressions)
                END AS CTR
            FROM base
        ),
        
        
        CRP_Calc AS (
            SELECT
                *,
                CASE
                    WHEN Conversion_Rate > 0.10 THEN 'High'
                    WHEN Conversion_Rate >= 0.05 THEN 'Medium'
                    ELSE 'Low'
                END AS Conversion_Category
            FROM CTR_Calc
        ),
        
        
        ROI_Calc AS (
            SELECT
                *,
                CASE
                    WHEN ROI > 5 THEN 5
                    WHEN ROI < -1 THEN -1
                    ELSE ROI
                END AS Adjusted_ROI,
                CASE
                    WHEN ROI >= 1 THEN 'Low Risk'
                    WHEN ROI >= 0 THEN  'Medium Risk'
                    ELSE 'High Risk'
                END AS ROI_Risk_Level
            FROM CRP_Calc
        )
        
        
        SELECT 
            Campaign_Type, 
            COUNT(*) AS Total_Campaigns,
            AVG(CTR) AS Avg_CTR,
            AVG(Conversion_Rate) AS Avg_Conversion_Rate, 
            AVG(Adjusted_ROI) AS Avg_Adjusted_ROI,
            SUM(Clicks) AS Total_Clicks,
            SUM(Impressions) AS Total_Impressions,
            SUM(CASE WHEN Conversion_Category = 'High' THEN 1 ELSE 0 END) AS High_Performing_Count,
            SUM(CASE WHEN ROI_Risk_Level = 'High Risk' THEN 1 ELSE 0 END) AS High_Risk_Count,          
            MAX(metrics_timestamp) AS Latest_Metrics_Timestamp
        FROM ROI_Calc
        GROUP BY Campaign_Type
    """

    df_new_metrics = spark.sql(sql_query)
    new_metrics_count = df_new_metrics.count()

    logger.info(f"SQL Metrics generated rows : {new_metrics_count}")
    logger.info("\n" + df_new_metrics._jdf.showString(25, 25, False))
    logger.info(f"SQL Metrics Columns Count : {len(df_new_metrics.columns)}")
    logger.info(f"SQL Metrics Columns Name : {df_new_metrics.columns}")


    # Incremental logic
    if valid_parquet_data(SQL_METRICS_PATH):
        logger.info("Existing SQL Metrics detected → Incremental load")

        df_existing = spark.read.parquet(SQL_METRICS_PATH)

        df_incremental = df_new_metrics.join(
            df_existing.select("Campaign_Type"),
            on="Campaign_Type",
            how="left_anti"
        )

        inc_count = df_incremental.count()
        logger.info(f"New SQL Metrics rows to insert : {inc_count}")

        if inc_count == 0:
            logger.info("No new SQL Metrics → unchanged")
            return

        df_final = df_existing.unionByName(df_incremental)

    else:
        logger.info("First SQL Metrics load")
        df_final = df_new_metrics

    # Safe overwrite
    df_final.write.mode(write_mode).parquet(SQL_METRICS_PATH)
    logger.info("SQL Metrics saved successfully...")


if __name__ == "__main__":
    sql_metrics()