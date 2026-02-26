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

# PARAMETERS CELL ********************

pipelineName =""
pipelineParams={}
targetWorkspace=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check type before loading
if isinstance(pipelineParams, str):  # Ensure it's a string before parsing
    jsonParams = json.loads(pipelineParams)
else:
    jsonParams = pipelineParams

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use targetWorkspace if provided, otherwise None (current workspace)
target_ws = targetWorkspace if targetWorkspace and targetWorkspace.strip() else None
status = RunPipeline(pipelineName, jsonParams, target_ws)
# status = "Failed"

if status != "Completed":
    raise ValueError(status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
