#schemas/schemas.py
from config.spark_session import get_spark
from config.path import RAW_DATA_PATH
from utils.logger import get_logger


def explore_schema():
    logger = get_logger(
        name = "MarketingCampaign Schema",
        log_file = "schemas.log"
    )

    # Initialize Spark
    spark = get_spark()

    #Reading raw CSV
    logger.info("Reading Raw CSV")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )


    #Dataset Schema
    logger.info("Dataset schemas (JSON)")
    logger.info(df.schema.json())


    #First 5 Data (Sample)
    logger.info("Sample data (first 5 rows)")
    for row in df.limit(5).toJSON().take(5):
        logger.info(row)


    #Number of Rows & Columns
    logger.info("Number of Rows & Columns")
    num_rows = df.count()
    num_columns = len(df.columns)

    logger.info(f"Number of rows : {num_rows}")
    logger.info(f"Number of columns : {num_columns}")


    #Show Column names
    logger.info("Column names")
    for col in df.columns:
        logger.info(f"Column : {col}")

    return df


if __name__ == "__main__":
    explore_schema()