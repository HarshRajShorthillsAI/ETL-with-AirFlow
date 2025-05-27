from dag_task import DAG_Task
from join_tables import Join_Tables

class Join_Operation_Part_2(Join_Tables):
    def __init__(self):
        super().__init__()

    def join_operations_part_2(self):
        self.join_tables(f"{DAG_Task.OUTPUT_DIR}/grouped_data.json", f"{DAG_Task.LOADED_JSON}/Features.json", cols=["TrimId"], join_type="full")
        self.join_tables(f"{DAG_Task.OUTPUT_DIR}/grouped_data.json", f"{DAG_Task.LOADED_JSON}/meta.json", cols=["TrimId"], join_type="full")
        return f"{DAG_Task.OUTPUT_DIR}/grouped_data.json"
    
if __name__ == "__main__":
    task = Join_Operation_Part_2()
    task.join_operations_part_2()
