# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run CustomLogging

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

model=""
batchExecution=""
notebook=""
notebookParams="{'model':'test', 'batchExecution': 'e81ec82d-9172-4800-b5f5-9a723997b250', 'otherParam':'1234'}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params = str(notebookParams).replace("'", '"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeLogStarted(notebook,"")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    result = notebookutils.notebook.run(notebook,500,json.loads(params))
    writeLogFinishedSuccess(notebook,"")
    # notebookutils.notebook.exit("Success")
except Exception as e:
    writeLogFinishedError(notebook,"",str(e))
    print(e)
    # notebookutils.notebook.exit("Failed")
    raise(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
