from dag_task import DAG_Task
from pyspark.sql.functions import col

class Join_With_Details_Table(DAG_Task):
    def __init__(self):
        super().__init__()

    def join_with_details_tables(self, df1_path:str, df2_path:str, cols, join_type):
        spark = self.spark_session
        
        try:
            df1 = spark.read.json(df1_path)
            df2 = spark.read.json(df2_path)

            [ print(x) for x in df1.columns]
            [ print(x) for x in df2.columns]
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
            
            print(f"[DEBUG] df1 count: {df1.count()}, size: {df1.rdd.map(lambda x: len(str(x))).sum()} bytes")
            print(f"[DEBUG] df2 count: {df2.count()}, size: {df2.rdd.map(lambda x: len(str(x))).sum()} bytes")

            df1 = df1.repartition("TrimId")
            df2 = df2.repartition("TrimId")

            result_df = df1.join(df2, on="TrimId", how="full")

            # df1.unpersist()
            # df2.unpersist()
            
            output_path = f"{DAG_Task.OUTPUT_DIR}/output.json"
            result_df.repartition('TrimId').write.mode("overwrite").json(output_path)
        except Exception as e:
            print(f"caught exception: {e}")
            raise   
        # spark.stop()
        return output_path
