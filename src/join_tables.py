from dag_task import DAG_Task
from pyspark.sql.functions import col
class Join_Tables(DAG_Task):
    def __init__(self):
        super().__init__()

    def join_tables(self, df1_path:str, df2_path:str, cols, join_type):
        spark = self.spark_session
        try:
            df1 = spark.read.json(df1_path)
            df2 = spark.read.json(df2_path)

            # Ensure join keys are same type (common reason for silent failure)
            for key in cols:
                df1 = df1.withColumn(key, col(key).cast("string"))
                df2 = df2.withColumn(key, col(key).cast("string"))

            df1_columns = set(df1.columns)
            df2_columns = set(df2.columns)

            # Identify non-key common columns and drop them from df2
            common_non_key_cols = df1_columns.intersection(df2_columns) - set(cols)

            if common_non_key_cols:
                print(f"[INFO] Dropping overlapping columns from second table: {common_non_key_cols}")
                df2 = df2.drop(*common_non_key_cols)
            else:
                print("[INFO] No overlapping columns found apart from join keys.")

            joined_df = df1.join(df2, on=cols, how=join_type)

            df1.unpersist()
            df2.unpersist()

            output_path = f"{DAG_Task.OUTPUT_DIR}/grouped_data.json"
            if joined_df.rdd.isEmpty():
                print("[WARNING] Joined DataFrame is empty. Skipping write operation.")
            else:
                joined_df.write.mode("overwrite").json(output_path)
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return output_path
