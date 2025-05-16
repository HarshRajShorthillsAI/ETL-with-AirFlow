import sys
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

class Spark_session:
    # DAG Configuration
    DATA_PATH = "file:///opt/airflow/data/CRS_Datafeed_2025-02-28_1"
    LOADED_JSON = f"{DATA_PATH}/json_files"
    OUTPUT_DIR = f"{DATA_PATH}/output"

    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

    def __init__(self):
        self.spark_session = self.create_spark_session()
        self.dict_df = {
            'Features': None,
            'logic': None,
            'Mfrs': None,
            'photogallery': None,
            'pkgs': None,
            'Trims': None
        }