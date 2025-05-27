from dag_task import DAG_Task
from pyspark.sql.functions import expr, array_sort, collect_set, struct

class Feature_Detail_Value(DAG_Task):
    def __init__(self):
        super().__init__()

    def extract_feature_detail_values(self, loaded_json_path:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            feature.printSchema()
            feature.show(5)

            filtered_feature = (
                feature.select(['TrimId', 'FeatureName', 'Value', 'AttributeName'])
                .groupBy('TrimId', 'FeatureName')
                .agg(expr("last(AttributeName) as AttributeName"), expr("last(Value) as Value"))
                .groupBy('TrimId')
                .agg(array_sort(collect_set(struct('FeatureName', 'AttributeName', 'Value'))).alias('Details'))
            )
            feature.unpersist()

            filtered_feature = self.convert_nested_array_to_json(filtered_feature)

            print("Filtered row count:", filtered_feature.count())

            output_path = f"{DAG_Task.LOADED_JSON}/details.json"
            # self.validate_df(filtered_feature, output_path)
            filtered_feature.write.mode('overwrite').json(output_path)
            filtered_feature.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path
    
if __name__ == "__main__":
    task = Feature_Detail_Value()
    task.extract_feature_detail_values()
