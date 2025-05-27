import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, expr
from pyspark.conf import SparkConf

class DAG_Task:
    DATA_PATH = "file:///opt/airflow/data/CRS_Datafeed_2025-02-28_1"
    LOADED_JSON = f"{DATA_PATH}/json_files"
    OUTPUT_DIR = f"{DATA_PATH}/output"

    def __init__(self):
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        self.spark_session = self.create_spark_session()
        self.dict_df = {
            'Features': None,
            'logic': None,
            'Mfrs': None,
            'photogallery': None,
            'pkgs': None,
            'Trims': None
        }

    # Initialize Spark session in local mode
    def create_spark_session(self):

        conf = SparkConf() \
        .set("spark.executor.heartbeatInterval", "10s") \
        .set("spark.network.timeout", "300s") \
        .set("spark.sql.debug.maxToStringFields", "100") \
        .set("spark.driver.maxResultSize", "1g") \
        .set("spark.speculation", "true") \
        # .set("spark.sql.shuffle.spill", "true") \
        # .set("spark.shuffle.spill.compress", "true") \
        # .set("spark.io.compression.codec", "lz4") \
        # .set("spark.memory.fraction", "0.6")

        return SparkSession.builder \
            .appName("Load CSV and Transform") \
            .master("local[*]") \
            .config(conf=conf) \
            .config("spark.driver.extraJavaOptions",
                "-XX:+UseGCOverheadLimit "
                "-XX:+HeapDumpOnOutOfMemoryError "
                "-XX:HeapDumpPath=/tmp/driver-heapdump.hprof "
                "-XX:+PrintGCDetails "
                "-XX:+PrintGCTimeStamps") \
            .config("spark.executor.extraJavaOptions",
                "-XX:+UseGCOverheadLimit "
                "-XX:+HeapDumpOnOutOfMemoryError "
                "-XX:HeapDumpPath=/tmp/executor-heapdump.hprof "
                "-XX:+PrintGCDetails "
                "-XX:+PrintGCTimeStamps") \
            .getOrCreate()
    
    def convert_nested_array_to_json(self, feature:DataFrame):
        # Correct transformation to JSON
        return feature.withColumn(
            "Details",
            to_json(
                expr(
                    """
                    map_from_arrays(
                        transform(Details, detail -> detail.FeatureName),
                        transform(Details, detail -> named_struct('label', detail.AttributeName, 'desc', detail.Value))
                    )
                    """
                )
            )
        )