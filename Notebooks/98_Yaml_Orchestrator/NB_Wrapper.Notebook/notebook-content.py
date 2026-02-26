# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run NB_CustomLogging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

notebook=""
notebookParams=""
batchExecution=""
processName = ""
targetWorkspace=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if notebook=="NB_RunPipeline":
    jsonParams={}
    jsonParams["pipelineParams"] = pipelineParams #PipelineParams is a parameter passed when notebook is "NB_RunPipeline"
    jsonParams["pipelineName"]=pipelineName
    jsonParams["targetWorkspace"]=targetWorkspace if targetWorkspace and targetWorkspace.strip() else ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check type before loading
if isinstance(notebookParams, str):  # Ensure it's a string before parsing
    params = str(notebookParams).replace("'", '"')
    if notebookParams!="":
        jsonParams = json.loads(notebookParams)
else:
    jsonParams = notebookParams

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeLogStarted(notebook,processName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if targetWorkspace and targetWorkspace.strip() and notebook != "NB_RunPipeline":
        # Cross-workspace notebook execution using Fabric REST API
        result = notebookutils.notebook.run(notebook, 500, jsonParams, targetWorkspace)
    else:
        # Local workspace execution or NB_RunPipeline (handled internally)
        result = notebookutils.notebook.run(notebook, 500, jsonParams)
    
    writeLogFinishedSuccess(notebook, processName)
    # notebookutils.notebook.exit("Success")
except Exception as e:
    writeLogFinishedError(notebook, processName, str(e))
    print(e)
    # notebookutils.notebook.exit("Failed")
    raise(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
