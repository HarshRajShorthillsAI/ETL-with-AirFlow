from dag_task import DAG_Task
from join_with_details_table import Join_With_Details_Table

class Join_Operation_Part_3(Join_With_Details_Table):
    def __init__(self):
        super().__init__()

    def join_operations_part_3(self):
        # join_tables(f"{OUTPUT_DIR}/grouped_data.json", f"{LOADED_JSON}/details.json", cols=["TrimId"], join_type="full")
        self.join_with_details_tables(f"{DAG_Task.OUTPUT_DIR}/grouped_data.json", f"{DAG_Task.LOADED_JSON}/details.json", cols=["TrimId"], join_type="full")
        return f"{DAG_Task.OUTPUT_DIR}/grouped_data.json"
    
if __name__ == "__main__":
    task = Join_Operation_Part_3()
    task.join_operations_part_3()