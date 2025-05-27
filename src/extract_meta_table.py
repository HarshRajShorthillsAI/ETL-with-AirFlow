from dag_task import DAG_Task

class Extract_Meta_Table(DAG_Task):
    def __init__(self):
        super().__init__()

    def extract_meta_for_trimId(self, loaded_json_path:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            filtered_meta = feature.filter(feature.FeatureName == 'Identifiers')\
                .filter(feature.AttributeName == 'Data Provider')\
                .select(['TrimId', 'Value'])\
                .withColumnRenamed('Value', 'Meta')
            feature.unpersist()
            output_path = f"{DAG_Task.LOADED_JSON}/meta.json"
            # self.validate_df(filtered_meta, output_path)
            filtered_meta.write.mode('overwrite').json(output_path)
            filtered_meta.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path
    
if __name__ == "__main__":
    task = Extract_Meta_Table()
    task.extract_meta_for_trimId()
