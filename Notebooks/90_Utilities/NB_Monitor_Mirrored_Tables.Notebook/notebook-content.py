# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "082b101a-c0f9-474c-91b7-93344e39e98e",
# META       "default_lakehouse_name": "lh_modernBI_fabricFramework_bronze_01",
# META       "default_lakehouse_workspace_id": "4fc684e0-5902-419b-bf6a-9de7fc814327",
# META       "known_lakehouses": [
# META         {
# META           "id": "082b101a-c0f9-474c-91b7-93344e39e98e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Mirrored Database Table Status Monitor
# 
# This notebook monitors all mirrored databases and identifies tables with "Failed" or "Running with warnings" status. Results are stored in the bronze lakehouse for monitoring and alerting.
# 
# ## Features:
# - Connects to Microsoft Fabric API to retrieve mirrored database information
# - Monitors table replication status across all mirrored databases
# - Stores problematic table information in Delta format
# 
# ## Prerequisites:
# - Variable Library `vl-tourtailors-01` with `mirroring_workspaceId` variable
# - Access to `lh_tourtailors_bronze_01` lakehouse
# - Appropriate permissions for Fabric API calls

# CELL ********************

# Import required libraries
import requests
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from pyspark.sql.functions import current_timestamp as spark_current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from datetime import datetime
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration and Variables

# Get the mirroring workspace ID from variable list
try:
    # vl = notebookutils.variableLibrary.getLibrary("vl-tourtailors-01")
    mirroring_workspace_id = "4fc684e0-5902-419b-bf6a-9de7fc814327"
    target_workspace_id = "4fc684e0-5902-419b-bf6a-9de7fc814327"
    target_lakehouse_id = "082b101a-c0f9-474c-91b7-93344e39e98e"
    print(f"Retrieved mirroring ID's: {mirroring_workspace_id}")
except Exception as e:
    print(f"Error retrieving mirroring workspace ID: {e}")
    raise

# Target table configuration
target_lakehouse = "lh_modernBI_fabricFramework_bronze_01"
target_table = "mirrored_tables_status"
target_table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/dbo/{target_table}"


# Status filters - tables with these statuses will be captured
problematic_statuses = ["Failed", "Running with warnings"]

print(f"Target lakehouse: {target_lakehouse}")
print(f"Target table: {target_table}")
print(f"Monitoring statuses: {', '.join(problematic_statuses)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Authentication and API Setup
print("\n Setting up Fabric API authentication...")

def get_fabric_access_token():
    """
    Get access token for Fabric API calls
    Uses the notebook's built-in authentication
    """
    try:
        # Use the fabric context to get access token
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com/")
        return token
    except Exception as e:
        print(f"Error getting access token: {e}")
        raise

# Get access token
access_token = get_fabric_access_token()
print("Successfully obtained access token")

# Setup API headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Fabric API base URL
fabric_api_base = "https://api.fabric.microsoft.com/v1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get all mirrored databases in workspace
def get_mirrored_databases(workspace_id):
    """
    Retrieve all mirrored databases from the specified workspace
    """
    print(f"\n Retrieving mirrored databases from workspace: {workspace_id}")
    
    try:
        url = f"{fabric_api_base}/workspaces/{workspace_id}/items"
        params = {
            "type": "MirroredDatabase"
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            databases = response.json().get("value", [])
            print(f"Found {len(databases)} mirrored databases")
            
            for db in databases:
                print(f"   {db['displayName']} (ID: {db['id']})")
                
            return databases
        else:
            print(f"Error retrieving mirrored databases: {response.status_code}")
            print(f"Response: {response.text}")
            return []
            
    except Exception as e:
        print(f"Exception retrieving mirrored databases: {e}")
        return []

# Get all mirrored databases
mirrored_databases = get_mirrored_databases(mirroring_workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get tables status from a mirrored database
def get_mirrored_tables_status(workspace_id, database_id, database_name):
    """
    Get the status of all tables in a mirrored database
    """
    print(f"\n Getting table statuses for database: {database_name}")
    
    try:
        url = f"{fabric_api_base}/workspaces/{workspace_id}/mirroredDatabases/{database_id}/getTablesMirroringStatus"
        
        response = requests.post(url, headers=headers)
        
        if response.status_code == 200:
            response_data = response.json()
            tables = response_data.get("data", [])
            print(f"    Found {len(tables)} tables in {database_name}")
            
            table_statuses = []
            problematic_count = 0
            
            for table in tables:
                metrics = table.get("metrics", {})
                table_info = {
                    "database_name": database_name,
                    "database_id": database_id,
                    "table_name": table.get("sourceTableName", ""),
                    "table_id": "",  # Not available in this API response
                    "status": table.get("status", ""),
                    "last_sync_time": metrics.get("lastSyncDateTime", ""),
                    "source_schema": table.get("sourceSchemaName", ""),
                    "source_table": table.get("sourceTableName", ""),
                    "source_object_type": table.get("sourceObjectType", ""),
                    "processed_bytes": metrics.get("processedBytes", 0),
                    "processed_rows": metrics.get("processedRows", 0),
                    "last_sync_latency_seconds": metrics.get("lastSyncLatencyInSeconds", 0)
                }
                
                # Check if this table has a problematic status
                if table_info["status"] in problematic_statuses:
                    problematic_count += 1
                    print(f"    {table_info['table_name']}: {table_info['status']}")
                
                table_statuses.append(table_info)
            
            print(f"    Tables with issues: {problematic_count}/{len(tables)}")
            return table_statuses
            
        else:
            print(f"    Error retrieving tables for {database_name}: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"    Exception retrieving tables for {database_name}: {e}")
        return []

# Get table statuses for all mirrored databases
all_table_statuses = []

for db in mirrored_databases:
    db_name = db["displayName"]
    db_id = db["id"]
    
    if db_name!="Configs":
        table_statuses = get_mirrored_tables_status(mirroring_workspace_id, db_id, db_name)
        all_table_statuses.extend(table_statuses)
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)

print(f"\n Total tables processed: {len(all_table_statuses)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_mirrored_tables_status2(workspace_id, database_id, database_name):
    """
    Get the status of all tables in a mirrored database
    """
    print(f"\n Getting table statuses for database: {database_name}")
    
    try:
        url = f"{fabric_api_base}/workspaces/{workspace_id}/mirroredDatabases/{database_id}/getTablesMirroringStatus"
        
        response = requests.post(url, headers=headers)
        
        if response.status_code == 200:
            response_data = response.json()
            tables = response_data.get("data", [])
            print(response_data)
            print(f"    Found {len(tables)} tables in {database_name}")
            
            table_statuses = []
            problematic_count = 0
            
            for table in tables:
                metrics = table.get("metrics", {})
                table_info = {
                    "database_name": database_name,
                    "database_id": database_id,
                    "table_name": table.get("sourceTableName", ""),
                    "table_id": "",  # Not available in this API response
                    "status": table.get("status", ""),
                    "last_sync_time": metrics.get("lastSyncDateTime", ""),
                    "source_schema": table.get("sourceSchemaName", ""),
                    "source_table": table.get("sourceTableName", ""),
                    "source_object_type": table.get("sourceObjectType", ""),
                    "processed_bytes": metrics.get("processedBytes", 0),
                    "processed_rows": metrics.get("processedRows", 0),
                    "last_sync_latency_seconds": metrics.get("lastSyncLatencyInSeconds", 0)
                }
                
                # # Check if this table has a problematic status
                # if table_info["status"] in problematic_statuses:
                #     problematic_count += 1
                print(f"    {table_info['table_name']}: {table_info['status']}")
                
                table_statuses.append(table_info)
            
            print(f"    Tables with issues: {problematic_count}/{len(tables)}")
            return table_statuses
            
        else:
            print(f"    Error retrieving tables for {database_name}: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"    Exception retrieving tables for {database_name}: {e}")
        return []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_statuses2 = get_mirrored_tables_status2("4fc684e0-5902-419b-bf6a-9de7fc814327", "dc07797d-aea4-4d0f-98cf-9f6eba8f3dbb", "sqldbdevstuff")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter and prepare data for storage
print("\n Filtering tables with problematic statuses...")

# Filter tables with problematic statuses
problematic_tables = [
    table for table in all_table_statuses 
    if table["status"] in problematic_statuses
]

print(f" Found {len(problematic_tables)} tables with issues:")
for table in problematic_tables:
    print(f"   {table['database_name']}.{table['table_name']}: {table['status']}")

# Add monitoring metadata
current_datetime = datetime.now()
monitoring_run_id = f"run_{current_datetime.strftime('%Y%m%d_%H%M%S')}"

# Prepare data for DataFrame
if problematic_tables:
    # Add monitoring metadata to each record
    for table in problematic_tables:
        table["monitoring_timestamp"] = current_datetime.isoformat()
        table["monitoring_run_id"] = monitoring_run_id
        table["workspace_id"] = mirroring_workspace_id
        
    # Convert to pandas DataFrame first for easier handling
    df_pandas = pd.DataFrame(problematic_tables)
    
    print(f"\n Prepared {len(df_pandas)} records for storage")
    print("Columns:", list(df_pandas.columns))
else:
    print("\n No problematic tables found - creating empty DataFrame")
    # Create empty DataFrame with expected schema
    empty_data = {
        "database_name": [],
        "database_id": [],
        "table_name": [],
        "table_id": [],
        "status": [],
        "last_sync_time": [],
        "source_schema": [],
        "source_table": [],
        "monitoring_timestamp": [],
        "monitoring_run_id": [],
        "workspace_id": []
    }
    df_pandas = pd.DataFrame(empty_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to Spark DataFrame and define schema
print("\n Converting to Spark DataFrame...")

# Define the schema for the target table
schema = StructType([
    StructField("database_name", StringType(), True),
    StructField("database_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("table_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_sync_time", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("monitoring_timestamp", StringType(), True),
    StructField("monitoring_run_id", StringType(), True),
    StructField("workspace_id", StringType(), True)
])

# Convert pandas DataFrame to Spark DataFrame
if len(df_pandas) > 0:
    # Ensure proper data types for numeric columns
    df_pandas['processed_bytes'] = pd.to_numeric(df_pandas['processed_bytes'], errors='coerce').fillna(0).astype('int64')
    df_pandas['processed_rows'] = pd.to_numeric(df_pandas['processed_rows'], errors='coerce').fillna(0).astype('int64') 
    df_pandas['last_sync_latency_seconds'] = pd.to_numeric(df_pandas['last_sync_latency_seconds'], errors='coerce').fillna(0).astype('int32')
    
    # Create Spark DataFrame without schema first, then convert types
    df_spark = spark.createDataFrame(df_pandas)
    
    # Cast to proper types to match schema
    df_spark = df_spark.select(
        col("database_name").cast("string"),
        col("database_id").cast("string"), 
        col("table_name").cast("string"),
        col("table_id").cast("string"),
        col("status").cast("string"),
        col("last_sync_time").cast("string"),
        col("source_schema").cast("string"),
        col("source_table").cast("string"),
        col("source_object_type").cast("string"),
        col("processed_bytes").cast("long"),
        col("processed_rows").cast("long"),
        col("last_sync_latency_seconds").cast("int"),
        col("monitoring_timestamp").cast("string"),
        col("monitoring_run_id").cast("string"),
        col("workspace_id").cast("string")
    )
else:
    # Create empty Spark DataFrame with schema
    df_spark = spark.createDataFrame([], schema=schema)

# Add additional computed columns
df_final = df_spark.withColumn("load_date", current_date()) \
                  .withColumn("load_timestamp", spark_current_timestamp())

print(f" Spark DataFrame created with {df_final.count()} records")

# Show sample data
print("\n Sample data:")
df_final.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data to target table
print(f"\n Writing data to target table: {target_table}")

try:
    df_final.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(target_table_path)
    
    print(f" Successfully wrote {df_final.count()} records to {target_table}")
    
    # Verify the write by reading back
    verification_df = spark.read.format("delta").load(target_table_path)
    total_records = verification_df.count()
    
    print(f" Total records in {target_table}: {total_records}")
    
    # Show latest records from this monitoring run
    latest_records = verification_df.filter(col("monitoring_run_id") == monitoring_run_id)
    print(f" Records from current run ({monitoring_run_id}): {latest_records.count()}")
    
except Exception as e:
    print(f" Error writing to target table: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
