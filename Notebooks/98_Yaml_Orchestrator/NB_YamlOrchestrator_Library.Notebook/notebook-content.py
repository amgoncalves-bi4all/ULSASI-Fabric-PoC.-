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
import yaml_orchestrator_lib as orchestrator
from fabric_common_lib.common_functions import *
import yaml
import uuid
import json

commonlib.configure_fabric_globals(notebookutils)
orchestrator.configure_fabric_globals(notebookutils)

# Initialize Fabric context
commonlib.initialize_fabric_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#Parameters definition

#Name of the model
model = "framework"
modelReset = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceId, bronze_lakehouse_id, _, _ = commonlib.GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve the variable library
vl = notebookutils.variableLibrary.getLibrary("vl-modernBI-fabricFramework-01")

try:
    eventhouseName = vl.eventhouseName
    databaseName = vl.eventhouseDatabase
except:
    eventhouseName = ""
    databaseName = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (eventhouseName != ""):
    commonlib.SetSessionLogURI(eventhouseName, databaseName)
    spark.conf.set(f"UseYaml","true")
    log_eventhouse_id = spark.conf.get("fabric.log_eventhouse_id", None)
    log_kqldatabase_id = spark.conf.get("fabric.log_kqldatabase_id", None)
    kustoURI = spark.conf.get("kustoUri", None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batchExecution=str(uuid.uuid4())
status_map = {}

if not modelReset:
    df_LastExecution = orchestrator.GetLastModelExecution(model)
    # Convert dataframe to a dict {processInternalId -> status}
    status_map = {row["processInternalId"]: row["status"] for row in df_LastExecution.collect()}

    # Check if dataframe has rows
    if df_LastExecution.head(1):
        # Get first row's batchExecution
        batchExecution = df_LastExecution.first()["batchExecution"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def CallPipeline(sourceSystemName, sourceObjectName):
#     print(f"Executing pipeline for {sourceSystemName}.{sourceObjectName}")

# def CallNotebook(notebookName, dependsOn):
#     print(f"Executing notebook {notebookName} with dependencies {dependsOn}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load YAML file
yaml_file_path = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/config/{model}.yml"

config = orchestrator.load_yaml_config(yaml_file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sections = ["raw", "silver", "gold"]
dag_activities = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dag_activities = orchestrator.generate_dag_from_config(
    config,
    model,
    workspaceId,
    bronze_lakehouse_id,
    log_kqldatabase_id,
    kustoURI,
    sections,
    timeoutInSeconds=43200,
    concurrency=4
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = dag_activities
print(json.dumps(DAG, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# notebookutils.notebook.runMultiple(["NotebookSimple", "NotebookSimple2"])
if notebookutils.notebook.validateDAG(DAG):
    notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# activities = [
#     {
#         "name": row['notebookName']+'_'+row['destinationObjectPattern'],
#         "path": row['notebookName'],
#         "timeoutPerCellInSeconds": 90,
#         "args": {"modelName":modelName},
#         "dependencies": [] if row['dependents'] == [""] else row['dependents']
#     } for row in raw_section.items()
# ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
