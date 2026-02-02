#main.py
#Master Execution Pipeline File
from schemas.schema import explore_schema
from ingestion.bronze_ingestion import bronze_ingestion
from profiling.data_profiling import data_profiling
from processed.cleaning.silver_cleaning import silver_cleaning
from processed.filter_final.silver_filter_final import silver_filter_final
from transformations.sql_metrics import sql_metrics
from aggregation.gold_agg import gold_aggregation
from incremental.incremental_load import incremental_load
from validation.data_validation import validate_data
from utils.logger import get_logger


def main():

    logger = get_logger(
        name = "ETL_Main_Pipeline",
        log_file = "main_pipeline.log"
    )

    logger.info("Marketing Campaign Pipeline Started...")

    try:

        # 1. Schema Exploration
        logger.info("Step 1: Schema Exploration Started")
        explore_schema()
        logger.info("Step 1 Completed")

        # 2. Raw -> Bronze Layer
        logger.info("Step 2: Raw to Bronze Ingestion Started")
        bronze_ingestion()
        logger.info("Step 2 Completed")

        # 3. Data Profiling
        logger.info("Step 3: Data Profiling Started")
        data_profiling()
        logger.info("Step 3 Completed")

        # 4. Bronze -> Silver Cleaning
        logger.info("Step 4: Silver Cleaning Started")
        silver_cleaning()
        logger.info("Step 4 Completed")

        # 5. Silver Cleaned -> Silver Final (Business Filters)
        logger.info("Step 5: Silver Final Filtering Started")
        silver_filter_final()
        logger.info("Step 5 Completed")

        # 6. SQL Metrics
        logger.info("Step 6: SQL Metrics Started")
        sql_metrics()
        logger.info("Step 6 Completed")

        # 7. Gold Aggregation
        logger.info("Step 7: Gold Aggregation Started")
        gold_aggregation()
        logger.info("Step 7 Completed")

        # 8. Incremental Load
        logger.info("Step 8: Incremental Loading Started")
        incremental_load()
        logger.info("Step 8 Completed")

        # 9. Data Validation
        logger.info("Step 9: Data validation Started")
        validate_data()
        logger.info("Step 9 Completed")


        logger.info("Marketing Campaign Pipeline Completed Successfully...")

    except Exception as e:
        logger.error(f"PIPELINE FAILED : {str(e)}")
        raise


if __name__ == "__main__":
    main()



