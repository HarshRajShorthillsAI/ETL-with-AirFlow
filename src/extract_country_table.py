from dag_task import DAG_Task
from pyspark.sql.functions import col, array

class Extract_Country_Table(DAG_Task):
    def __init__(self):
        super().__init__()

    def extract_country_from_feature_table(self, loaded_json_path:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            feature_dataframe = (
                feature.filter(feature.AttributeName == 'Manufacturer Country')
                .withColumnRenamed('Value', 'countries')
                .withColumn('countries', array(col('countries')))
                .drop(*['PackageId', 'AttributeId', 'FeatureName', 'AttributeName'])
            )
            output_path = f"{DAG_Task.OUTPUT_DIR}/countries.json"
            # self.validate_df(feature_dataframe, output_path)
            feature.unpersist()
            feature_dataframe.write.mode('overwrite').json(output_path)
            feature_dataframe.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path
    
if __name__ == "__main__":
    task = Extract_Country_Table()
    task.extract_country_from_feature_table()
