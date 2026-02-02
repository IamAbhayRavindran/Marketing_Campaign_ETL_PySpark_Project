#config/spark_session.py
from config.env import pyspark_env
from pyspark.sql import SparkSession

pyspark_env()
def get_spark():
        spark = (
                SparkSession.builder
                .master("local[1]")
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.sql.parquet.writeLegacyFormat", "true")  # safer for Parquet
                .config("spark.sql.sources.commitProtocolClass",
                        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")  # critical fix
                .config("spark.hadoop.fs.permissions.enabled", "false")  # disable native permission checks
                .getOrCreate()
        )

        return spark