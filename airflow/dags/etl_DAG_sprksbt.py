from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

class SparkETLDag:
    def __init__(self):
        self.dag_id = 'spark_submit_etl_dag'
        self.schedule_interval = '@daily'
        self.start_date = datetime(2024, 4, 1)
        self.catchup = False

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
            description='ETL pipeline using Spark with SparkSubmitOperator',
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            catchup=self.catchup,
            tags=['spark', 'etl'],
        )

    def spark_submit_task(self, task_id, application=None, application_args=None):
        return SparkSubmitOperator(
            task_id=task_id,
            application=f'./src/{application}',  # Update path as needed
            name=task_id,
            conn_id='spark_default',
            application_args=application_args or [],
            executor_memory='4g',
            total_executor_cores=4,
            verbose=True,
            dag=self.dag
        )

    def create_dag(self):
        with self.dag:

            # Load trims data
            load_trims_data = self.spark_submit_task(
                task_id='run_my_spark_job',
                application='load_data.py',
                application_args=['Trims']
            )

            # Rename trim table data
            rename_trim_table_data = self.spark_submit_task(
                task_id='rename_trim_table_job',
                application='rename_trim_table.py'
            )

            # Load features data
            load_features_data = self.spark_submit_task(
                task_id='load_features_data_job',
                application='load_data.py',
                application_args=['Features']
            )

            # Extract country from feature table data
            extract_country = self.spark_submit_task(
                task_id='extract_country_table_job',
                application='extract_country_table.py'
            )

            # Extract options for trimId data
            extract_options = self.spark_submit_task(
                task_id='extract_options_table_job',
                application='extract_options_table.py'
            )

            # # Extract features for trimId data
            extract_features = self.spark_submit_task(
                task_id='extract_features_table_job',
                application='extract_trimid_features_table.py'
            )

            # Extract meta for TrimId data
            extract_meta = self.spark_submit_task(
                task_id='extract_meta_table_job',
                application='extract_meta_table.py'
            )

            # Extract feature detail values data
            extract_feature_values = self.spark_submit_task(
                task_id='extract_feature_detail_job',
                application='extract_feature_details.py'
            )

            # Join all data 1
            join_1 = self.spark_submit_task(
                task_id='join_operation_part_1_job',
                application='join_operation_part_1.py'    
            )

            # Join all data 2
            join_2 = self.spark_submit_task(
                task_id='join_operation_part_2_job',
                application='join_operation_part_2.py'
            )

            # Join all data 3
            join_3 = self.spark_submit_task(
                task_id='join_operation_part_3_job',
                application='join_operation_part_3.py'
            )

            # Generate output
            generate_output = self.spark_submit_task(
                task_id='generate_output_job',
                application='generate_output.py'
            )

            # Define task dependencies
            load_trims_data >> rename_trim_table_data >> load_features_data >> [extract_country, extract_feature_values]
            extract_country >> extract_meta
            extract_feature_values >> extract_features
            extract_features >> extract_options
            [extract_meta, extract_options] >> join_1 >> join_2 >> join_3
            join_3 >> generate_output

            # load_trims_data >> rename_trim_table_data

        return self.dag


# Instantiate the DAG
dag = SparkETLDag().create_dag()
