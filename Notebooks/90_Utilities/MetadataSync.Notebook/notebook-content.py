# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2f85ade8-66ff-4460-a85e-956734b2c1ce",
# META       "default_lakehouse_name": "lh_modernBI_fabricFramework_gold_01",
# META       "default_lakehouse_workspace_id": "d2ff9d9d-3c53-4cd6-9526-7a417c5789b9"
# META     }
# META   }
# META }

# CELL ********************

#### Import needed libraries
import json
import time
import struct
import sqlalchemy
import pyodbc
import notebookutils
import pandas as pd
from pyspark.sql import functions as fn
from datetime import datetime
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### Functions
def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#### Parameters
# Use either workspace name or workspaceId
workspace_name = ""
workspace_id = ""

# Pass lakehouse name to refresh metadata only on that lakehouse. If empty, all lakehouses metadata will be refreshed
lakehouse_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Global variables
all_workspaces = fabric.FabricRestClient().get(f"/v1/workspaces").json()["value"]

filtered_workspace = []
if len(workspace_name) > 0:
    filtered_workspace = [d for d in all_workspaces if d['displayName'] == workspace_name][0]

    print(filtered_workspace)

    ## not needed, but usefull
    # tenant_id=spark.conf.get("trident.tenant.id")
    workspace_id=spark.conf.get("trident.workspace.id")
    # lakehouse_id=spark.conf.get("trident.lakehouse.id")
    # lakehouse_name=spark.conf.get("trident.lakehouse.name")

    # workspace_id = notebookutils.runtime.context.get("defaultLakehouseWorkspaceId")
    # lakehouse_id = notebookutils.runtime.context.get("defaultLakehouseId")
    # lakehouse_name = notebookutils.runtime.context.get("defaultLakehouseName")

    if len(filtered_workspace) > 0:
        workspace_id = filtered_workspace["id"]
else:
    if len(workspace_id)==0:
        raise Exception("No workspace name or id provided")

user_name = notebookutils.runtime.context.get("userName")

#Instantiate the client
client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def syncLakehouse(lh):

    # This is the SQL endpoint I want to sync with the lakehouse, this needs to be the GUI
    sqlendpoint_id = lh["properties"]['sqlEndpointProperties']['id']
    print("Lakehouse name: ", lh["displayName"], " | ", sqlendpoint_id)

    # URI for the call
    uri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint_id}"
    # This is the action, we want to take
    payload = {"commands":[{"$type":"MetadataRefreshExternalCommand"}]}

    # Call the REST API
    response = client.post(uri,json= payload)
    ## You should add some error handling here

    # return the response from json into an object we can get values from
    data = json.loads(response.text)

    # We just need this, we pass this to call to check the status
    batchId = data["batchId"]

    # the state of the sync i.e. inProgress
    progressState = data["progressState"]

    # URL so we can get the status of the sync
    statusuri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint_id}/batches/{batchId}"

    statusresponsedata = ""

    while progressState == 'inProgress' :
        # For the demo, I have removed the 1 second sleep.
        time.sleep(1)

        # check to see if its sync'ed
        #statusresponse = client.get(statusuri)

        # turn response into object
        statusresponsedata = client.get(statusuri).json()

        # get the status of the check
        progressState = statusresponsedata["progressState"]
        # show the status
        display(f"Sync state: {progressState}")

    # if its good, then create a temp results, with just the info we care about
    if progressState == 'success':
        table_details = [
            {
            'tableName': table['tableName'],
            'warningMessages': table.get('warningMessages', []),
            'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
            'tableSyncState':  table['tableSyncState'],
            'sqlSyncState':  table['sqlSyncState']
            }
            for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus']
        ]

    # if its good, then shows the tables
    if progressState == 'success':
        # Print the extracted details
        print("Extracted Table Details:")
        for detail in table_details:
            print(f"- Table: {pad_or_truncate_string(detail['tableName'],30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}")

        if len(table_details) == 0:
            print("No tables available!")

    ## if there is a problem, show all the errors
    if progressState == 'failure':
        # display error if there is an error
        display(statusresponsedata)

    print("_"*150)
    print("\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(lakehouse_name)==0:
    ws_lakehouses = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses").json()["value"]

    #### Iterate over all available LH of the selected WS, and refresh the data

    print("_"*150)
    print("User name: ", user_name)
    print("Workspace name: ", fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}").json()["displayName"], " | ", workspace_id)
    print("\n")

    for lh in ws_lakehouses:
        syncLakehouse(lh)

else:
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name).id
    lh = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()

    syncLakehouse(lh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
