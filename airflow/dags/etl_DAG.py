import os
import dotenv
from src.ingestion import *
from airflow.decorators import dag, task
from datetime import datetime, timedelta

dotenv.load_dotenv()

@dag(schedule=None, default_args={
    'start_date': datetime(2023, 1, 1),  # Set an appropriate start date for your DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}, catchup=False, 
    tags=['pyspark', 'data_transformation'])
def etl_example():
    
    dotenv.load_dotenv('/app/.env')
    import logging
    logging.info([ os.getenv("DB_NAME"),
            "localhost",
            os.getenv("DB_PORT"),
            os.getenv("DB_TABLE"),
            os.getenv("DB_SCHEMA"),
            os.getenv("DB_USER"),
            os.getenv("DB_PASS"),])
    logging.info( os.path.join(os.path.dirname(__file__))  )

    DATA_PATH = "/opt/airflow/data/CRS_Datafeed_2025-02-28_1/"

    
    
    @task
    def load_csv_data(data_path=DATA_PATH):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return load_data(data_path)

    @task
    def rename_trim_table_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        print(f"inside rename trim table")
        return rename_trim_table(parquet_path)

    @task
    def extract_country_from_feature_table_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return extract_country_from_feature_table(parquet_path)

    @task
    def extract_options_for_trimId_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return extract_options_for_trimId(parquet_path)

    @task
    def extract_features_for_trimId_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return extract_features_for_trimId(parquet_path)

    @task
    def extract_meta_for_TrimId_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return extract_meta_for_trimId(parquet_path)

    @task
    def extract_feature_detail_values_data(parquet_path:str):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return extract_feature_detail_values(parquet_path)

    @task
    def join_tables_data(dataframe_1_path, dataframe_2_path, cols, join_type):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return join_tables(dataframe_1_path, dataframe_2_path, cols, join_type)

    @task
    def generate_output_data(data):
        """
        Loads csv file from docker into pyspark dataframe
        """
        return generate_output(data)
    
    # @task
    # def print_output(output_data):
    #     with open('output.json', 'w') as f:
    #         json.dump(output_data, f, indent=4)

    # Invoke functions to create tasks and define dependencies
    # write_to_db_task(extract_and_load_task())
    parquet_path = load_csv_data()
    print(f"running rename trims table data now...")
    trims_table_path = rename_trim_table_data(parquet_path)
    country_table_path = extract_country_from_feature_table_data(parquet_path)
    # options_path = extract_options_for_trimId_data(parquet_path)
    feature_path = extract_features_for_trimId_data(parquet_path)
    meta_path = extract_meta_for_TrimId_data(parquet_path)
    details_path = extract_feature_detail_values_data(parquet_path)
    grouped_data_path = join_tables_data(trims_table_path, country_table_path, cols=['TrimId'], join_type="full")
    grouped_data_path = join_tables_data(grouped_data_path, country_table_path, cols=['TrimId', 'countries'], join_type="full")
    # grouped_data_path = join_tables_data(grouped_data_path, options_path, cols=['TrimId'], join_type="full")
    grouped_data_path = join_tables_data(grouped_data_path, feature_path, cols=['TrimId'], join_type='full')
    grouped_data_path = join_tables_data(grouped_data_path, meta_path, cols=['TrimId'], join_type='full')
    grouped_data_path = join_tables_data(grouped_data_path, details_path, cols=['TrimId'], join_type='full')
    generate_output_data(grouped_data_path)
    # print_output(grouped_data_path)

etl_example()