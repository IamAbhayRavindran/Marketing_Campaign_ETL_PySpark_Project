# ingestion/bronze_ingestion.py
import os
from pyspark.sql.functions import input_file_name, current_timestamp
from config.spark_session import get_spark
from config.path import RAW_DATA_PATH, BRONZE_DATA_PATH
from utils.logger import get_logger


def parquet_exists(path):
    return (
        os.path.exists(path) and
        any(f.endswith(".parquet") for f in os.listdir(path))
    )

def bronze_ingestion():
    spark = get_spark()

    logger = get_logger(
        name="Bronze Ingestion",
        log_file="bronze_ingestion.log"
    )

    logger.info("Bronze Ingestion Started...")

    # 1. Read RAW CSV Data
    logger.info("Reading Raw CSV Data...")

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_DATA_PATH)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    raw_count = raw_df.count()
    logger.info(f"Total Raw rows detected : {raw_count}")

    # 2. Read existing Bronze rows (if exists)
    if parquet_exists(BRONZE_DATA_PATH):
        bronze_existing_df = spark.read.parquet(BRONZE_DATA_PATH)
        bronze_existing_count = bronze_existing_df.count()
    else:
        bronze_existing_count = 0

    # 3. Calculate new rows since last Bronze ingestion
    new_rows = raw_count - bronze_existing_count
    if new_rows > 0:
        logger.info(f"New rows since last Bronze ingestion : {new_rows}")
    else:
        logger.info("No new rows since last Bronze ingestion")

    # 4. Handle empty RAW
    if raw_count == 0:
        logger.warning("RAW folder is empty. Bronze will be emptied.")
        spark.createDataFrame([], raw_df.schema) \
            .write.mode("overwrite").parquet(BRONZE_DATA_PATH)
        return

    # 5. Write RAW to Bronze (overwrite)
    raw_df.write.mode("overwrite").parquet(BRONZE_DATA_PATH)

    logger.info("Bronze Ingestion Completed Successfully...")
    return raw_df

if __name__ == "__main__":
    bronze_ingestion()
