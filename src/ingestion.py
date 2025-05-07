import pyspark.sql.functions as f

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import initcap, concat_ws, col, array, collect_set, struct, array_sort, to_json, expr
from pyspark.sql import DataFrame
import json
import sys
import os

class Ingestion:
    
    # DAG Configuration
    DATA_PATH = "file:///opt/airflow/data/CRS_Datafeed_2025-02-28_1"
    LOADED_JSON = f"{DATA_PATH}/json_files"
    OUTPUT_DIR = f"{DATA_PATH}/output"

    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

    def __init__(self):
        self.spark_session = self.create_spark_session()
        self.dict_df = {
            'Features': None,
            'logic': None,
            'Mfrs': None,
            'photogallery': None,
            'pkgs': None,
            'Trims': None
        }
    
    # Initialize Spark session in local mode
    def create_spark_session(self):

        conf = SparkConf() \
        .set("spark.executor.heartbeatInterval", "10s") \
        .set("spark.network.timeout", "300s") \
        .set("spark.sql.debug.maxToStringFields", "100") \
        .set("spark.driver.maxResultSize", "1g") \
        .set("spark.speculation", "true")  # speculative execution can help dead executors

        return SparkSession.builder \
            .appName("Load CSV and Transform") \
            .master("local[*]") \
            .config(conf=conf) \
            .config("spark.driver.extraJavaOptions",
                "-XX:+UseGCOverheadLimit "
                "-XX:+HeapDumpOnOutOfMemoryError "
                "-XX:HeapDumpPath=/tmp/driver-heapdump.hprof "
                "-XX:+PrintGCDetails "
                "-XX:+PrintGCTimeStamps") \
            .config("spark.executor.extraJavaOptions",
                "-XX:+UseGCOverheadLimit "
                "-XX:+HeapDumpOnOutOfMemoryError "
                "-XX:HeapDumpPath=/tmp/executor-heapdump.hprof "
                "-XX:+PrintGCDetails "
                "-XX:+PrintGCTimeStamps") \
            .getOrCreate()

    def validate_df(self, df, df_name="DataFrame"):
        if df.rdd.isEmpty():
            raise ValueError(f"{df_name} is empty.")
        print(f"{df_name} schema:\n{df.printSchema()}")
        df.show(5, truncate=False)

    def load_data(self, data_path:str=DATA_PATH, key_:str=None, loaded_json:str=LOADED_JSON):
        spark = self.create_spark_session()

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

    def rename_trim_table(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
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
            output_path = f"{self.OUTPUT_DIR}/Trims.json"
            self.validate_df(trim_dataframe, output_path)
            trim_dataframe.write.mode('overwrite').json(output_path)
            trim_dataframe.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def extract_country_from_feature_table(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            feature_dataframe = (
                feature.filter(feature.AttributeName == 'Manufacturer Country')
                .withColumnRenamed('Value', 'countries')
                .withColumn('countries', array(col('countries')))
                .drop(*['PackageId', 'AttributeId', 'FeatureName', 'AttributeName'])
            )
            output_path = f"{self.OUTPUT_DIR}/countries.json"
            self.validate_df(feature_dataframe, output_path)
            feature.unpersist()
            feature_dataframe.write.mode('overwrite').json(output_path)
            feature_dataframe.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def extract_options_for_trimId(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            filtered_feature = feature.filter(feature.Value == 'Optional') \
                .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Options'))
            feature.unpersist()
            output_path = f"{self.OUTPUT_DIR}/options.json"
            self.validate_df(filtered_feature, output_path)
            filtered_feature.write.mode('overwrite').json(output_path)
            filtered_feature.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def extract_features_for_trimId(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            filtered_feature = feature.filter(feature.Value == 'Standard')\
                .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Features'))
            feature.unpersist()
            output_path = f"{self.OUTPUT_DIR}/feature.json"
            self.validate_df(filtered_feature, output_path)
            filtered_feature.write.mode('overwrite').json(output_path)
            filtered_feature.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def extract_meta_for_trimId(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
        try:
            feature = spark.read.json(f"{loaded_json_path}/Features.json")
            filtered_meta = feature.filter(feature.FeatureName == 'Identifiers')\
                .filter(feature.AttributeName == 'Data Provider')\
                .select(['TrimId', 'Value'])\
                .withColumnRenamed('Value', 'Meta')
            feature.unpersist()
            output_path = f"{self.LOADED_JSON}/meta.json"
            self.validate_df(filtered_meta, output_path)
            filtered_meta.write.mode('overwrite').json(output_path)
            filtered_meta.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def convert_nested_array_to_json(self, feature:DataFrame):
        # Correct transformation to JSON
        return feature.withColumn(
            "Details",
            to_json(
                expr(
                    """
                    map_from_arrays(
                        transform(Details, detail -> detail.FeatureName),
                        transform(Details, detail -> named_struct('label', detail.AttributeName, 'desc', detail.Value))
                    )
                    """
                )
            )
        )

    def extract_feature_detail_values(self, loaded_json_path:str=LOADED_JSON):
        spark = self.create_spark_session()
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

            output_path = f"{self.LOADED_JSON}/details.json"
            self.validate_df(filtered_feature, output_path)
            filtered_feature.write.mode('overwrite').json(output_path)
            filtered_feature.unpersist()
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return loaded_json_path

    def join_tables(self, df1_path:str, df2_path:str, cols, join_type):
        spark = self.create_spark_session()
        try:
            df1 = spark.read.json(df1_path)
            df2 = spark.read.json(df2_path)

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

            joined_df = df1.join(df2, on=cols, how=join_type)

            df1.unpersist()
            df2.unpersist()

            output_path = f"{self.OUTPUT_DIR}/grouped_data.json"
            if joined_df.rdd.isEmpty():
                print("[WARNING] Joined DataFrame is empty. Skipping write operation.")
            else:
                joined_df.write.mode("overwrite").json(output_path)
        except Exception as e:
            print(f"caught exception: {e}")
            raise
        # spark.stop()
        return output_path

    def join_with_details_tables(self, df1_path:str, df2_path:str, cols, join_type):
        spark = self.create_spark_session()
        
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
            
            df1 = df1.repartition("TrimId")
            df2 = df2.repartition("TrimId")

            result_df = df1.join(df2, on="TrimId", how="full")

            df1.unpersist()
            df2.unpersist()
            
            output_path = f"{self.OUTPUT_DIR}/output.json"
            result_df.write.mode("overwrite").json(output_path)
        except Exception as e:
            print(f"caught exception: {e}")
            raise   
        # spark.stop()
        return output_path


    def join_operations_part_1(self):
        self.join_tables(f"{self.LOADED_JSON}/Trims.json", f"{self.LOADED_JSON}/countries.json", cols=["TrimId"], join_type="full")
        self.join_tables(f"{self.OUTPUT_DIR}/grouped_data.json", f"{self.LOADED_JSON}/Trims.json", cols=["TrimId"], join_type="full")
        return f"{self.OUTPUT_DIR}/grouped_data.json"
        
    def join_operations_part_2(self):
        self.join_tables(f"{self.OUTPUT_DIR}/grouped_data.json", f"{self.LOADED_JSON}/Features.json", cols=["TrimId"], join_type="full")
        self.join_tables(f"{self.OUTPUT_DIR}/grouped_data.json", f"{self.LOADED_JSON}/meta.json", cols=["TrimId"], join_type="full")
        return f"{self.OUTPUT_DIR}/grouped_data.json"

    def join_operations_part_3(self):
        # join_tables(f"{OUTPUT_DIR}/grouped_data.json", f"{LOADED_JSON}/details.json", cols=["TrimId"], join_type="full")
        self.join_with_details_tables(f"{self.OUTPUT_DIR}/grouped_data.json", f"{self.LOADED_JSON}/details.json", cols=["TrimId"], join_type="full")
        return f"{self.OUTPUT_DIR}/grouped_data.json"

    def generate_output(self, final_data_path:str=f"{OUTPUT_DIR}/grouped_data.json"):
        spark = self.create_spark_session()
        try:
            final_data = spark.read.json(final_data_path)

            rows = final_data.toLocalIterator()
            result = []

            for row in rows:
                # trimId = row["TrimId"]

                product_detail = {
                    "general" : {
                        "ProdType": row["ProdType"].upper() if row["ProdType"] else None,
                        "TrimId": row["TrimId"] if row["TrimId"] else None,
                        "ModelId": row["ModelId"] if row["ModelId"] else None,
                        "MakeId": row["MakeId"] if row["MakeId"] else None,
                        "manufacturer": row["manufacturer"] if row["manufacturer"] else None,
                        "model": row["model"] if row["model"] else None,
                        "year": row["year"] if row["year"] else None,
                        "msrp": row["msrp"] if row["msrp"] else None,
                        "description": row["description"] if row["description"] else None,
                        "TrimName": row["TrimName"] if row["TrimName"] else None,
                        "ModelName": row["ModelName"] if row["ModelName"] else None,
                        "category": row["category"] if row["category"] else None,
                        "subcategory": row["subcategory"] if row["subcategory"] else None,
                        "countries": row["countries"] if row["countries"] else None,
                    },
                    "meta": row["Meta"] if row["Meta"] else None,
                    # "options": row["Options"] if row["Options"] else None, # since task for retrieving options is throwing error resulting in task failure, kept it out
                    "features": row["Features"] if row["Features"] else None,
                }

                # Merge details into product_detail
                if row["Details"]:
                    product_detail.update({
                        f"{feature}": value for feature, value in json.loads(row["Details"]).items()
                    })

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
    import os
    import dotenv
    dotenv.load_dotenv()

    ingestion = Ingestion()

    trimmed_table = ingestion.rename_trim_table(ingestion.dict_df["Trims"])
    countries_table = ingestion.extract_country_from_feature_table(ingestion.dict_df["Features"])
    options_table = ingestion.extract_options_for_trimId(ingestion.dict_df["Features"])
    feature_table = ingestion.extract_features_for_trimId(ingestion.dict_df["Features"])
    meta_table = ingestion.extract_meta_for_trimId(ingestion.dict_df["Features"])
    other_table = ingestion.extract_feature_detail_values(ingestion.dict_df['Features'])
    final_data = ingestion.join_tables(trimmed_table, countries_table, col=["TrimId"], join_type="full")
    final_data = ingestion.join_tables(final_data, options_table, col=["TrimId"], join_type="full")
    final_data = ingestion.join_tables(final_data, feature_table, col=["TrimId"], join_type="full")
    final_data = ingestion.join_tables(final_data, meta_table, col=["TrimId"], join_type="full")
    final_data = ingestion.join_tables(final_data, other_table, col=["TrimId"], join_type="full")
    output_data = ingestion.generate_output(final_data)
    with open('output.json', 'w') as f:
        json.dump(output_data, f, indent=4)