from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 19),
    'retries': 1,
}

# Create DAG
with DAG(
    dag_id='spark_csv_etl',
    default_args=default_args,
    description='ETL with Spark to load and transform CSV',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Run Spark job using SparkSubmitOperator
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/airflow/dags/spark_job.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.master": "local[*]"  # Run in local mode with all CPU cores
        }
    )

    run_spark_job
