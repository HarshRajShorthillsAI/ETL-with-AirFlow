from pyspark.sql import SparkSession

def run_spark_job():
    # Initialize Spark session in local mode
    spark = SparkSession.builder \
        .appName("Load CSV and Transform") \
        .master("local[*]") \
        .getOrCreate()

    # Define input and output paths
    input_path = "/opt/airflow/data/input_data.csv"
    output_path = "/opt/airflow/data/output/transformed_data.parquet"

    # Load CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print("✅ CSV Data Loaded Successfully")

    # Optional transformation (e.g., filter rows where 'status' is active)
    df_filtered = df.filter(df['status'] == 'active')
    print("✅ Data Transformed Successfully")

    # Write transformed data to Parquet
    df_filtered.write.mode('overwrite').parquet(output_path)
    print(f"✅ Transformed Data Saved Successfully at {output_path}")

    spark.stop()
