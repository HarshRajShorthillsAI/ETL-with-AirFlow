from dag_task import DAG_Task
import sys

class Load_Data(DAG_Task):

    def __init__(self):
        super().__init__()

    def load_data(self, data_path:str=DAG_Task.DATA_PATH, key_:str=None, loaded_json:str=DAG_Task.LOADED_JSON):
        spark = self.spark_session

        try:
            temp_df = spark.read.option("header", "true").csv(f"{data_path}/{key_}.csv")
            # validate_df(dict_df[key], f"{loaded_json}/{key}.json")
            temp_df.write.mode('overwrite').json(f"{loaded_json}/{key_}.json")
            temp_df.unpersist()
            print(f"Loaded {key_} data to JSON")
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("CSV file path not provided!")
    
    csv_path = sys.argv[1]

    task = Load_Data()
    task.load_data(key_=csv_path)
