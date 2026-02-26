# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# %run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def getNotebookId(notebookName):
    notebooks = fabric.list_items().query("Type == 'Notebook'")
    notebook = notebooks[notebooks["Display Name"]==notebookName]
    notebookId = notebook['Id'].iloc[0]
    return notebookId

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Init the sempy client
client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def callNotebook(notebookId):
    workspaceId = fabric.get_workspace_id()
    itemId = f"{notebookId}"
    jobType = f"RunNotebook"
    payload = {
                "executionData": {
                    # "parameters": {
                    #     "YourParamter1": 1,
                    #     "YourParamter2": "Param2"
                    # },
                    "configuration": {
                        "useStarterPool": False,
                        "useWorkspacePool": "defaultPool"
                    }
                }
            }
        
    response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType={jobType}",json= payload)

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Run with payload / pipeline parameters
responseTest1 = callNotebook(getNotebookId("TestNotebook1"))
responseTest2 = callNotebook(getNotebookId("TestNotebook2"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

responseJson = json.loads(getResponse.text)
print(responseJson)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
