# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {
# META       "environmentId": "ad124846-ec87-bf43-4c8c-41e424d66895",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import fabric_common_lib as commonlib
from fabric_common_lib.common_functions import *
from fabric_common_lib.standardized_common_functions import *

commonlib.configure_fabric_globals(notebookutils, mssparkutils)

# Initialize Fabric context
commonlib.initialize_fabric_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#Parameters definition

#Name of the model in the config table
modelName = "framework"
#Name of the source object in the config table
objectName = "Sales.Store"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# commonlib.SetSessionLogURI("eh_modernBI_fabricFramework_01", "eh_modernBI_fabricFramework_01")
# spark.conf.set(f"UseYaml","true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate dictionary to access silver config metadata
config_items = commonlib.GetConfigMetadata(modelName, objectName, "Silver")

# Generate source file paths based on the extract_type
config_items["sourceFilesPath"] = commonlib.GetLakehouseSourcePath(
    modelName, config_items.get("sourceLocationName"), objectName, config_items.get("extractType")
)
loadType = config_items["loadType"]
sourceFilesPath = config_items.get("sourceFilesPath")

# Ensure the result is always a list
if isinstance(sourceFilesPath, str):
    sourceFilesPath = [sourceFilesPath]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract required values
keyColumns = config_items['keyColumns']
KeyColumnList = keyColumns.split(',') if keyColumns else None

partitionColumns = config_items['partitionColumns']
extractType = config_items['extractType'].lower()

#Path of the destination table
destFilePath = config_items['destinationObjectPattern']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Read Sources and Add Metadata Columns###

# Create source dataframe
tableDF = spark.read.option("multiline", "true").parquet(*sourceFilesPath)

# Flatten and explode array type or struct columns, if required
tableDF = commonlib.FlattenArrayAndStruct(tableDF)

sourceDF = commonlib.AddMetadataColumnsStandardizedLayer(tableDF,loadType,KeyColumnList)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Write to table / File ###
print("Table : " + destFilePath )

if sourceDF.count() != 0:
    if loadType == 'overwrite':
        FullLoad(sourceDF, destFilePath, partitionColumns)
        print("Load Type in silver layer : " + loadType)

    if loadType == 'append':
        SnapshotLoad(sourceDF, destFilePath, partitionColumns, extractType)
        print("Load Type in silver layer : " + loadType )

    if loadType == 'merge':
        UpsertLoad(sourceDF, destFilePath, partitionColumns, extractType, KeyColumnList)
        print("Load Type in silver layer : " + loadType )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
