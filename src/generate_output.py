from dag_task import DAG_Task
import json

class Generate_Output(DAG_Task):
    def __init__(self):
        super().__init__()

    def generate_output(self, final_data_path:str=f"{DAG_Task.OUTPUT_DIR}/grouped_data.json"):
        spark = self.spark_session
        try:
            final_data = spark.read.json(final_data_path)

            rows = final_data.toLocalIterator()
            result = []

            flag=1

            for row in rows:
                # trimId = row["TrimId"]
                if flag:
                    print(f"row\n{row}")
                    flag=0

                product_detail = {
                    "general" : {
                        "ProdType": row["ProdType"].upper() if row["ProdType"] else None,
                        "TrimId": row["TrimId"] if row["TrimId"] else None,
                        "ModelId": row["Modelid"] if row["Modelid"] else None,
                        "MakeId": row["Makeid"] if row["Makeid"] else None,
                        "manufacturer": row["ManufacturerName"] if row["ManufacturerName"] else None,
                        "model": row["ModelName"] if row["ModelName"] else None,
                        "year": row["ModelYear"] if row["ModelYear"] else None,
                        "msrp": row["MSRP"] if row["MSRP"] else None,
                        "description": row["AttributeName"] if row["AttributeName"] else None,
                        "TrimName": row["TrimName"] if row["TrimName"] else None,
                        "ModelName": row["ModelName"] if row["ModelName"] else None,
                        "category": row["Value"] if row["Value"] else None,
                        "subcategory": row["ProdType"] if row["ProdType"] else None,
                        "countries": row["countries"] if row["countries"] else None,
                    },
                    "meta": row["Meta"] if row["Meta"] else None,
                    # "options": row["Options"] if row["Options"] else None, # since task for retrieving options is throwing error resulting in task failure, kept it out
                    "features": row["FeatureName"] if row["FeatureName"] else None,
                }

                # Merge details into product_detail
                # if row.get("Details"):
                #     product_detail.update({
                #         f"{feature}": value for feature, value in json.loads(row["Details"]).items()
                #     })

                result.append(product_detail)
                import gc
                gc.collect()
        except Exception as e:
            print(f"caught exception: {e}")
            raise

        spark = self.create_spark_session()
        with open("output.json", "w") as f:
            json.dump(result, f, indent=4)

if __name__ == "__main__":
    task = Generate_Output()
    task.generate_output()

