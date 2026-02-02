#profiling/data_profiling.py
from pyspark.sql.functions import regexp_replace, col, sum
from config.spark_session import get_spark
from config.path import BRONZE_DATA_PATH
from utils.logger import get_logger


def data_profiling():
    logger = get_logger(
        name = "Data Profiling",
        log_file = "profiling.log"
    )

    # Initialize Spark
    spark = get_spark()

    # Disable vectorized reader (safe for schema inspection)
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    #Read Bronze parquet
    logger.info("Reading Bronze parquet Data...")
    df = (
        spark.read
        .parquet(BRONZE_DATA_PATH)
    )

    #Dataset Schema
    logger.info("Dataset Schema (JSON)")
    logger.info(df.schema.json())

    #Data samples
    logger.info("Sample Data (first 5 rows)")
    for row in df.limit(5).toJSON().take(5):
        logger.info(row)

    #Null Values
    logger.info("Null values check")

    null_counts_df = df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    null_counts = null_counts_df.collect()[0].asDict()

    #Checking any NULL values exists
    if any(count>0 for count in null_counts.values()):
        logger.info("NULL values")
        for column, count in null_counts.items():
            if count > 0:
                logger.info(f"Column '{column}' has {count} NULL values")
    else:
        logger.info("No NULL values found in any column")


    #Duplicate campaigns (based on campaign_id)
    duplicates = (
        df.groupBy("Campaign_ID")
          .count()
          .filter(col("count") > 1)
    )

    # Log Duplicates
    dup_rows = duplicates.collect()

    if dup_rows:
        logger.info("Duplicate Check : Campaign_ID")
        for row in dup_rows:
            logger.info(
                f"Campaign_ID {row['Campaign_ID']} appears {row['count']} times"
            )
    else:
        logger.info("No duplicate Campaign_ID found")


    # Invalid numeric values based on column
    numeric_columns = [
        "Campaign_ID", "Conversion_Rate", "Acquisition_Cost",
        "ROI", "Clicks", "Impressions", "Engagement_Score"
    ]

    invalid_numeric_df = df.filter(
        " OR ".join([f"{c} < 0" for c in numeric_columns])
    )

    invalid_count = invalid_numeric_df.count()

    if invalid_count > 0:
        logger.info(f"Invalid numeric values found :{invalid_count} rows")
        # Log each invalid column count
        for c in numeric_columns:
            count = df.filter(col(c) < 0).count()
            if count > 0:
                logger.info(f"Column '{c}' has {count} negative values")

        logger.info("Sample invalid rows : ")
        logger.info(invalid_numeric_df.show(5, truncate=False))
    else:
        logger.info("No invalid numeric values (negative numbers) found")


    # Calculations...

    # Total record count
    total_records = df.count()
    logger.info(f"Total records : {total_records}")


    # Columns name and count
    column_name = df.columns
    columns_count = len(df.columns)
    logger.info(f"Columns name : {column_name}")
    logger.info(f"Columns count : {columns_count}")


    # Company names & count
    companies_name = [row["Company"] for row in df.select("Company").distinct().collect()]
    companies_name_count = df.select("Company").distinct().count()
    logger.info(f"Total companies : {companies_name}")
    logger.info(f"Total companies count : {companies_name_count}")


    # Distinct campaign type & count
    campaigns_types = [row["Campaign_Type"] for row in df.select("Campaign_Type").distinct().collect()]
    campaigns_types_count =   df.select("Campaign_Type").distinct().count()
    logger.info(f"Total campaigns types : {campaigns_types}")
    logger.info(f"Total campaigns types count : {campaigns_types_count}")


    # Targeting Audience & count
    target_audience = [row["Target_Audience"] for row in df.select("Target_Audience").distinct().collect()]
    target_audience_count = df.select("Target_Audience").distinct().count()
    logger.info(f"Total target audience : {target_audience}")
    logger.info(f"Total target audience count : {target_audience_count}")


    # Duration & count
    duration = [row["Duration"] for row in df.select("Duration").distinct().collect()]
    duration_count = df.select("Duration").distinct().count()
    logger.info(f"Total duration : {duration}")
    logger.info(f"Total duration count : {duration_count}")


    # Channels & count
    channels = [row["Channel_Used"] for row in df.select("Channel_Used").distinct().collect()]
    channels_count = df.select("Channel_Used").distinct().count()
    logger.info(f"Total channels : {channels}")
    logger.info(f"Total channels count : {channels_count}")


    # Location & count
    Location = [row["Location"] for row in df.select("Location").distinct().collect()]
    location_count = df.select("Location").distinct().count()
    logger.info(f"Total location : {Location}")
    logger.info(f"Total location count : {location_count}")


    # Customer_segment and count
    customer_segment = [row["Customer_Segment"] for row in df.select("Customer_Segment").distinct().collect()]
    customer_segment_count = df.select("Customer_Segment").distinct().count()
    logger.info(f"Total customer_segment : {customer_segment}")
    logger.info(f"Total customer_segment count : {customer_segment_count}")



if __name__ == "__main__":
    data_profiling()