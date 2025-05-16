from dag_task import DAG_Task
from pyspark.sql.functions import initcap, concat_ws, col, array, collect_set, struct, array_sort, to_json, expr

class Rename_Trim_Table(DAG_Task):
    def __init__(self):
        super().__init__()

    def rename_trim_table(self, loaded_json_path:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session
        try:
            trim = spark.read.json(f"{loaded_json_path}/Trims.json")
            trim_dataframe = (
                trim.withColumnRenamed('ManufacturerName', 'manufacturer')
                .withColumnRenamed('ModelYear', 'year')
                .withColumnRenamed('MSRP', 'msrp')
                .withColumn('model', concat_ws(" ", col("ModelName"), col("TrimName")))
                .withColumn('category', initcap(col("ProdType")))
                .withColumn('subcategory', initcap(col("ProdType")))
                .withColumn('description', concat_ws(" ", col("manufacturer"), col("model")))
                .select(['ProdType', 'TrimId', 'ModelId', 'MakeId', 'manufacturer', 'model', 'year', 'msrp',
                            'description', 'TrimName', 'ModelName', 'category', 'subcategory'])
            )
            trim.unpersist()
            output_path = f"{DAG_Task.OUTPUT_DIR}/Trims.json"
            # self.validate_df(trim_dataframe, output_path)
            trim_dataframe.write.mode('overwrite').json(output_path)
            trim_dataframe.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path
    
if __name__ == "__main__":
    task = Rename_Trim_Table()
    task.rename_trim_table()
