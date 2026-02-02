#data_read/file_read.py
import os
from config.path import (
RAW_DATA_PATH,
BRONZE_DATA_PATH,
SILVER_CLEAN_DATA_PATH, SILVER_FILTER_FINAL_PATH,
SQL_METRICS_PATH,
GOLD_BASE_PATH, GOLD_CAMPAIGN_SUMMARY, GOLD_CHANNEL_SUMMARY, GOLD_DAILY_SUMMARY
)
from config.spark_session import get_spark
from utils.logger import get_logger


def file_read(
    path: str,
    file_format: str = "parquet",
    show_rows: int = 10,
    header: bool = True,
    infer_schema: bool = True
):
    """
    Generic Spark file reader.

    Args:
        path (str): File or directory path
        file_format (str): parquet | csv | json
        show_rows (int): Number of rows to display
        header (bool): CSV header option
        infer_schema (bool): Infer schema (CSV)

    Returns:
        DataFrame or None
    """

    logger = get_logger(
        name="File Reader",
        log_file="file_reader.log"
    )

    spark = get_spark()
    spark.catalog.clearCache()

    logger.info(f"Starting file read → Path: {path}")
    logger.info(f"File format: {file_format}")

    # Path validation
    if not os.path.exists(path):
        logger.error(f"Path does not exist: {path}")
        return None

    # Read logic
    try:
        if file_format.lower() == "parquet":
            df = spark.read.parquet(path)

        elif file_format.lower() == "csv":
            df = (
                spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .csv(path)
            )

        elif file_format.lower() == "json":
            df = spark.read.json(path)

        else:
            logger.error(f"Unsupported file format: {file_format}")
            return None

    except Exception as e:
        logger.exception(f"Failed to read file: {e}")
        return None

    # Row count
    row_count = df.count()
    logger.info(f"Total rows: {row_count}")

    if row_count == 0:
        logger.warning("File is empty")
        return df

    #Columns Name & Count
    columns_names = df.columns
    logger.info(f"Column Names: {columns_names}")
    logger.info(f"Columns Count: {len(columns_names)}")

    if columns_names == 0:
        logger.warning("File is empty")
        return df



    # Show sample
    logger.info(f"Showing top {show_rows} rows")
    df.show(show_rows, truncate=False)

    # Schema
    logger.info("Schema (JSON)")
    logger.info(df.schema.json())

    return df


if __name__ == "__main__":
    # Example usage
    file_read(
        path=GOLD_DAILY_SUMMARY,
        file_format="parquet",
        show_rows=10
    )

