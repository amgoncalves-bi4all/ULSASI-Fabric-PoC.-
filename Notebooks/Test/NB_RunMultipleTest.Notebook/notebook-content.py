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
# META     }
# META   }
# META }

# CELL ********************

import json
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

model="test"
batchExecution=str(uuid.uuid4())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eventhouseName = "eh_modernBI_fabricFramework_01"
databaseName = "eh_modernBI_fabricFramework_01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def setSessionLogURI(eventhouseName, databaseName):
    if (eventhouseName != "" and databaseName != ""):
        workspaceId=spark.conf.get("trident.workspace.id")
        client = fabric.FabricRestClient()
        ws_eventhouses = fabric.FabricRestClient().get(f"/v1/workspaces/{workspaceId}/eventhouses").json()["value"]

        eh_Logging = [eh for eh in ws_eventhouses if eh["displayName"]==eventhouseName]
        kustoUri = eh_Logging[0]["properties"]["queryServiceUri"]

        spark.conf.set(f"kustoUri",kustoUri)
        spark.conf.set(f"kustoDatabase",databaseName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setSessionLogURI(eventhouseName, databaseName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# f"{'model':'{model}', 'batchExecution': '{batchExecution}', 'otherParam':'1234'}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

activities = [
    {
        "name": "TestNotebook1",
        "path": "TestWrapper",
        "timeoutPerCellInSeconds": 90,
        "args": {"notebook": "TestNotebook1",
                 "notebookParams": "{'model':'test', 'batchExecution': 'e81ec82d-9172-4800-b5f5-9a723997b250', 'otherParam':'1234'}"
                },
        "dependencies": ["TestNotebook2"]
    },
    {
        "name": "TestNotebook2",
        "path": "TestWrapper",
        "timeoutPerCellInSeconds": 90,
        "args": {"notebook": "TestNotebook2",
                 "notebookParams": "{'model':'test', 'batchExecution': 'e81ec82d-9172-4800-b5f5-9a723997b250'}"
                }
    }
]

DAG = {
    "activities": activities,
    "timeoutInSeconds": 43200,  # max timeout for the entire DAG
    "concurrency": 50  # max number of notebooks to run concurrently
}

# print(json.dumps(DAG, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(json.dumps(DAG, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exceptionResult=""

if notebookutils.notebook.validateDAG(DAG):
    results = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
    
    for notebook_name, result in results.items():
        if result is None or "Failed" in str(result):  # If the notebook crashes, result may be None
            raise Exception(f"{notebook_name} failed unexpectedly: {result}")

    # try:
    #     notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
    # except Exception as e:
    #     exceptionResult = e.result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for notebook, details in exceptionResult.items():
    if details["exception"] is not None:  # Check if an exception exists
        print(f"Notebook: {notebook}")
        print(f"Error: {details['exception']}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exceptionResult["TestNotebook2"]["exception"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
