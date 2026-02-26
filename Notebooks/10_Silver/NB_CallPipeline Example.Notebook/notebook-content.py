# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipelines = fabric.list_items().query("Type == 'DataPipeline'")
pipeline = pipelines[pipelines["Display Name"]=='pl_dummy']
pipelineId = pipeline['Id'].iloc[0]
pipelineId

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# pipelines = fabric.list_items().query("Type == 'DataPipeline'")
# pipelines

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Init the sempy client
client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric REST API call “Job Scheduler – Run On Demand Item Job”
#    Docs: Job Scheduler - Run On Demand Item Job - REST API (Core) | https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job
#    HTTP: POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType={jobType}
#    Body/Payload: {"executionData": { ... } }

# Get the default workspace id of the notebook
workspaceId = fabric.get_workspace_id()
itemId = f"{pipelineId}"
jobType = f"Pipeline"
payload = {
            "executionData": {
                "parameters": {
                    "YourParamter1": 1,
                    "YourParamter2": "Param2"
                    }
                }
            }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Call Fabric REST API with semantic link Fabric REST client (sempy)
# Run without payload / no or default pipeline parameters

response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType={jobType}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run with payload / pipeline parameters
response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType={jobType}",json= payload)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from time import sleep
import json
# print(response.headers)
itemJobUrl = response.headers["Location"]
jobStatus=""

while jobStatus in ("InProgress", "NotStarted", ""):
    sleep(5)
    getResponse = client.get(itemJobUrl)
    jobStatus = json.loads(getResponse.text)["status"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

responseJson = json.loads(getResponse.text)
print(responseJson)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
