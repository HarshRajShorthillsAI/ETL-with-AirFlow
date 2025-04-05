import pyspark.sql.functions as f
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, array, collect_set, struct, array_sort, to_json, expr
from pyspark.sql import DataFrame
import os
import json

# Initialize Spark session in local mode
spark = SparkSession.builder \
    .appName("Load CSV and Transform") \
    .master("local[*]") \
    .config('spark.driver.memory','6g') \
    .config('spark.ui.showConsoleProgress', True) \
    .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    .getOrCreate()

dict_df = {
    "Features": None,
    "logic": None,
    "Mfrs": None,
    "photogallery": None,
    "pkgs": None,
    "Trims": None
}

# DAG Configuration
DATA_PATH = "file:///opt/airflow/data/CRS_Datafeed_2025-02-28_1"
LOADED_JSON = f"{DATA_PATH}/json_files"
OUTPUT_DIR = f"{DATA_PATH}/output"

def load_data(DATA_PATH)->str:

    dict_df = {}
    try:
        for key in ["Trims", "Features"]:
            temp_df = spark.read.option("header", "true").csv(f"{DATA_PATH}/{key}.csv")
            dict_df[key] = temp_df
            temp_df.write.mode('overwrite').json(f"{LOADED_JSON}/{key}.json")
            print(f"Loaded {key} data to JSON")
    except Exception as e:
        print(e)
        raise
    return LOADED_JSON

def rename_trim_table(loaded_json_path: str):
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
    output_path = f"{OUTPUT_DIR}/Trims.json"
    trim_dataframe.write.mode('overwrite').json(output_path)
    return output_path

def extract_country_from_feature_table(loaded_json_path: str):
    feature = spark.read.json(f"{loaded_json_path}/Features.json")
    feature_dataframe = (
        feature.filter(feature.AttributeName == 'Manufacturer Country')
        .withColumnRenamed('Value', 'countries')
        .withColumn('countries', array(col('countries')))
        .drop(*['PackageId', 'AttributeId', 'FeatureName', 'AttributeName'])
    )
    output_path = f"{OUTPUT_DIR}/countries.json"
    feature_dataframe.write.mode('overwrite').json(output_path)

    # Force execution of write by triggering an action
    # spark.read.parquet(output_path).count()

    return output_path

def extract_options_for_trimId(loaded_json_path:str):
    for root, dirs, files in os.walk(loaded_json_path):
        print(f"dirs: {dirs}\tfiles: {files}")
    feature = spark.read.json(f"{loaded_json_path}/Features.json")
    filtered_feature = feature.filter(feature.Value == 'Optional') \
        .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Options'))
    output_path = f"{OUTPUT_DIR}/options.json"
    filtered_feature.write.mode('overwrite').json(output_path)

    # Force execution of write by triggering an action
    # spark.read.parquet(output_path).count()

    return output_path

def extract_features_for_trimId(loaded_json_path:str):
    feature = spark.read.json(f"{loaded_json_path}/Features.json")
    filtered_feature = feature.filter(feature.Value == 'Standard')\
        .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Features'))
    output_path = f"{OUTPUT_DIR}/feature.json"
    filtered_feature.write.mode('overwrite').json(output_path)

    # Force execution of write by triggering an action
    # spark.read.parquet(output_path).count()

    return output_path

def extract_meta_for_trimId(loaded_json_path:str):
    feature = spark.read.json(f"{loaded_json_path}/Features.json")
    filtered_meta = feature.filter(feature.FeatureName == 'Identifiers')\
        .filter(feature.AttributeName == 'Data Provider')\
        .select(['TrimId', 'Value'])\
        .withColumnRenamed('Value', 'Meta')
    output_path = f"{LOADED_JSON}/meta.json"
    filtered_meta.write.mode('overwrite').json(output_path)

    # Force execution of write by triggering an action
    # spark.read.parquet(output_path).count()

    return output_path

def convert_nested_array_to_json(feature:DataFrame):
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

def extract_feature_detail_values(loaded_json_path: str):

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

    filtered_feature = convert_nested_array_to_json(filtered_feature)

    print("Filtered row count:", filtered_feature.count())

    output_path = f"{LOADED_JSON}/details.json"
    print("Writing to path:", output_path)
    filtered_feature.write.mode('overwrite').json(output_path)

    return output_path

def join_tables(df1_path:str, df2_path:str, cols, join_type):
    df1 = spark.read.json(df1_path)
    df2 = spark.read.json(df2_path)
    joined_df = df1.join(df2, on=cols, how=join_type)
    
    output_path = f"{OUTPUT_DIR}/grouped_data.json"
    joined_df.write.mode('overwrite').json(output_path)

    # Force execution of write by triggering an action
    # spark.read.parquet(output_path).count()

    return output_path

def generate_output(final_data_path):

    final_data = spark.read.json(final_data_path)

    rows = final_data.collect()
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

    with open("output.json", "w") as f:
        json.dump(result, f, indent=4)

if __name__ == "__main__":
    import os
    import dotenv
    dotenv.load_dotenv()

    trimmed_table = rename_trim_table(dict_df["Trims"])
    countries_table = extract_country_from_feature_table(dict_df["Features"])
    options_table = extract_options_for_trimId(dict_df["Features"])
    feature_table = extract_features_for_trimId(dict_df["Features"])
    meta_table = extract_meta_for_trimId(dict_df["Features"])
    other_table = extract_feature_detail_values(dict_df['Features'])
    final_data = join_tables(trimmed_table, countries_table, col=["TrimId"], join_type="full")
    final_data = join_tables(final_data, options_table, col=["TrimId"], join_type="full")
    final_data = join_tables(final_data, feature_table, col=["TrimId"], join_type="full")
    final_data = join_tables(final_data, meta_table, col=["TrimId"], join_type="full")
    final_data = join_tables(final_data, other_table, col=["TrimId"], join_type="full")
    output_data = generate_output(final_data)
    with open('output.json', 'w') as f:
        json.dump(output_data, f, indent=4)