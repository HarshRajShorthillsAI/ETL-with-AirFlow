import dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion import Ingestion

dotenv.load_dotenv()


class SparkETLDag:

    def __init__(self):
        self.dag_id = 'spark_etl_dag'
        self.schedule_interval = '@daily'
        self.start_date = datetime(2024, 4, 1)
        self.catchup = False
        self.ingestion = Ingestion()

        self.default_args = {
            'owner': 'admin',
            'depends_on_past': False,
            'email': ['harsh.raj@shorthills.ai'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

        self.dag = DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            description='ETL pipeline using Spark with PythonOperator',
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            catchup=self.catchup,
            tags=['spark', 'etl'],
        )

    def create_dag(self):
        with self.dag:

            load_trims_data = PythonOperator(
                task_id='load_trims_data',
                python_callable=self.ingestion.load_data,
                op_kwargs={"key_":"Trims"}
            )

            rename_trim_table_data = PythonOperator(
                task_id='rename_trim_table',
                python_callable=self.ingestion.rename_trim_table,
            )

            load_features_data = PythonOperator(
                task_id='load_features_data',
                python_callable=self.ingestion.load_data,
                op_kwargs={"key_":"Features"}
            )

            extract_country = PythonOperator(
                task_id='extract_country_from_feature_table_data',
                python_callable=self.ingestion.extract_country_from_feature_table,
            )

            extract_options = PythonOperator(
                task_id='extract_options_for_trimId_data',
                python_callable=self.ingestion.extract_options_for_trimId,
            )

            extract_features = PythonOperator(
                task_id='extract_features_for_trimId_data',
                python_callable=self.ingestion.extract_features_for_trimId,
            )

            extract_meta = PythonOperator(
                task_id='extract_meta_for_TrimId_data',
                python_callable=self.ingestion.extract_meta_for_trimId,
            )

            extract_feature_values = PythonOperator(
                task_id='extract_feature_detail_values_data',
                python_callable=self.ingestion.extract_feature_detail_values,
            )

            join_1 = PythonOperator(
                task_id='all_join_table_data_1',
                python_callable=self.ingestion.join_operations_part_1,
            )

            join_2 = PythonOperator(
                task_id='all_join_table_data_2',
                python_callable=self.ingestion.join_operations_part_2,
            )

            join_3 = PythonOperator(
                task_id="all_join_table_data_3",
                python_callable=self.ingestion.join_operations_part_3
            )

            # generate_output_task = PythonOperator(
            #     task_id='generate_output',
            #     python_callable=self.ingestion.generate_output
            # )

            # # Task dependencies
            # load_csv_data >> rename_trim_table_data >> [extract_country, extract_feature_values]
            # extract_country >> extract_meta
            # extract_feature_values >> extract_features
            # extract_features >> extract_options
            # [extract_meta, extract_options] >> join_1 >> join_2 >> generate_output_task

            load_trims_data >> rename_trim_table_data >> load_features_data >> [extract_country, extract_feature_values]
            extract_country >> extract_meta
            extract_feature_values >> extract_features
            extract_features >> extract_options
            [ extract_meta, extract_options ] >> join_1 >> join_2 >> join_3

        return self.dag

# Instantiate and assign the DAG object to a variable for Airflow to detect
dag = SparkETLDag().create_dag()