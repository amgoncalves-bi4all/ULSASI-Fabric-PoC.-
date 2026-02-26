# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "48ba6431-48ae-4daf-bcf2-5bf3f205c57d",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "25e297b2-d704-4f79-ad14-2a6ee28f7f53",
# META       "known_lakehouses": [
# META         {
# META           "id": "48ba6431-48ae-4daf-bcf2-5bf3f205c57d"
# META         },
# META         {
# META           "id": "2bcf7568-fa57-4c4c-89e4-333ed29a22e5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run Common_sql_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve the maximum ID from the config.copydata_config table to manually manage IDs,
# as the fabric warehouse does not support auto-increment (IDENTITY) columns.

get_max_id = spark.sql(
    "select max(id) as max_id from config.copydata_config"
).collect()[0]["max_id"]

identity = 0 if get_max_id is None else get_max_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# copydata_config table schema
schema = StructType(
    [
        StructField("model", StringType(), False),
        StructField("source_system_name", StringType(), False),
        StructField("source_system_type", StringType(), False),
        StructField("source_location_name", StringType(), True),
        StructField("source_object_name", StringType(), False),
        StructField("source_select_columns", StringType(), True),
        StructField("source_key_columns", StringType(), True),
        StructField("destination_system_name", StringType(), False),
        StructField("destination_system_type", StringType(), False),
        StructField("destination_object_pattern", StringType(), False),
        StructField("destination_directory_pattern", StringType(), False),
        StructField("destination_object_type", StringType(), False),
        StructField("extract_type", StringType(), False),
        StructField("delta_start_date", TimestampType(), True),
        StructField("delta_end_date", TimestampType(), True),
        StructField("delta_date_column", StringType(), True),
        StructField("delta_filter_condition", StringType(), True),
        StructField("flag_block", IntegerType(), False),
        StructField("block_size", IntegerType(), True),
        StructField("block_column", StringType(), True),
        StructField("flag_active", IntegerType(), False),
        StructField("create_date", TimestampType(), False),
        StructField("last_modified_date", TimestampType(), True),
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definition of static values
model = "Jira"
source_system_name = "Jira"
source_system_type = "Rest API"
source_location_name = ""  # generated inside the dynamic function if empty
source_object_list = ["table1", "table2", "table3"]  # Add as many tables as required
source_select_columns = None
source_key_columns = None
destination_system_name = "LH_Bronze"
destination_system_type = "lakehouse"
destination_object_pattern = ""  # generated inside the dynamic function if empty
destination_directory_pattern = f"{destination_system_name}/"
destination_object_type = "json"
extract_type = "delta"
delta_start_date = datetime.strptime("1900-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f")
delta_end_date = None
delta_date_column = "updated"
delta_filter_condition = None
flag_block = 0
block_size = None
block_column = None
flag_active = 1
create_date = datetime.now()
last_modified_date = datetime.now()

# Generate data dynamically using *args
data = [
    generate_dynamic_sql_data(
        model,
        source_system_name,
        source_system_type,
        source_location_name,
        source_object_name,
        source_select_columns,
        source_key_columns,
        destination_system_name,
        destination_system_type,
        destination_object_pattern,
        destination_directory_pattern,
        destination_object_type,
        extract_type,
        delta_start_date,
        delta_end_date,
        delta_date_column,
        delta_filter_condition,
        flag_block,
        block_size,
        block_column,
        flag_active,
        create_date,
        last_modified_date,
    )
    for source_object_name in source_object_list
]

df = spark.createDataFrame(data, schema=schema)

columns = [column.name for column in schema.fields]

df = df.withColumn(
    "id", row_number().over(Window.orderBy("model")) + lit(identity)
).select("id", *columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate the SQL statement
print(generate_sql_insert_all_rows(df, "admin.copydata_config"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
