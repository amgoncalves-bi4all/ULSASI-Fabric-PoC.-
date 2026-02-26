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

from pyspark.sql.functions import col, lit, round
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

model="modelname"
batchExecution="xcvbx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

processName = notebookutils.runtime.context["currentNotebookName"]
processInternalId = notebookutils.runtime.context["currentNotebookId"]
processType="notebook"
status="started"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-jpgqmvnphk0qyjp55m.z2.kusto.fabric.microsoft.com"

# The database to write the data
database = "eh_modernBI_fabricFramework_01"
# The table to write the data 
table    = "processExecutionLog"
# The access credentials for the write
accessToken = notebookutils.credentials.getToken('kusto')

df = spark.createDataFrame(
    [
        (model, batchExecution, processName, processInternalId, processType, status)
    ],
    ["Model", "BatchExecution", "ProcessName", "ProcessInternalId", "ProcessType", "Status"]  # add your column names here
)

# Add columns for ProcessName, CpuUsage, MemoryUsage, and Timestamp
df = df.withColumn("Timestamp", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# Write data to a Kusto table
df.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Kusto cluster uri to write the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-jpgqmvnphk0qyjp55m.z2.kusto.fabric.microsoft.com"

# The database to write the data
database = "eh_modernBI_fabricFramework_01"
# The table to write the data 
table    = "datesConfig"
# The access credentials for the write
accessToken = notebookutils.credentials.getToken('kusto')

model="framework"
tableName= "dim_something"
scope="DimType2"
startDate="2025-01-01"

# Convert startDate string to datetime
startDate_dt = datetime.strptime(startDate, "%Y-%m-%d")

# Create DataFrame
df = spark.createDataFrame(
    [(model, tableName, scope, startDate_dt)],  # Ensure correct data types
    ["model", "tableName", "scope", "startDate"]
)

# Add columns for ProcessName, CpuUsage, MemoryUsage, and Timestamp
df = df.withColumn("timestamp", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# Write data to a Kusto table
df.write.\
format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", accessToken ).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
