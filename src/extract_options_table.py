from dag_task import DAG_Task
from pyspark.sql.functions import collect_set
class Extract_Options_Table(DAG_Task):
    def __init__(self):
        super().__init__()
    
    def extract_options_for_trimId(self, loaded_json_path:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            filtered_feature = feature.filter(feature.Value == 'Optional') \
                .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Options'))
            feature.unpersist()
            output_path = f"{self.OUTPUT_DIR}/options.json"
            # self.validate_df(filtered_feature, output_path)
            filtered_feature.write.mode('overwrite').json(output_path)
            filtered_feature.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path
    
if __name__ == "__main__":
    task = Extract_Options_Table()
    task.extract_options_for_trimId()
