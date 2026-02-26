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

%run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

modelName = "framework"
layer = "Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceId, _, silver_lakehouse_id, _ = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Delta table locations
silver_gold_config_path = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/config/silverGoldConfig"
silver_gold_dependency_path = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/config/silverGoldDependency"


# Load Delta tables
tp_df = spark.read.format("delta").load(silver_gold_config_path)
t1_df = spark.read.format("delta").load(silver_gold_dependency_path)
t1_active_df = (t1_df.join(
    tp_df.select(col("destinationObjectPattern").alias('dependencyObjectName'), "model", "layer"), 
    ["dependencyObjectName", "model", "layer"], 
    "inner"
))
td_df = spark.read.format("delta").load(silver_gold_config_path)  # Same table as Tp but used in the join

# Apply filters for active rows and specific model/layer
tp_df = tp_df.filter((col("flagActive") == 1) & (col("model") == modelName) & (col("layer") == layer) & (col("notebookName").isNotNull()))
td_df = td_df.filter((col("flagActive") == 1) & (col("model") == modelName) & (col("layer") == layer) & (col("notebookName").isNotNull()))

# Perform LEFT JOINs
joined_df = tp_df.join(
    t1_active_df, 
    (tp_df["destinationObjectPattern"] == t1_active_df["objectName"]) & 
    (tp_df["model"] == t1_active_df["model"]) & 
    (tp_df["layer"] == t1_active_df["layer"]), 
    "left"
).join(
    td_df, 
    (t1_active_df["dependencyObjectName"] == td_df["destinationObjectPattern"]) & 
    (t1_active_df["model"] == td_df["model"]) & 
    (t1_active_df["layer"] == td_df["layer"]), 
    "left"
)

# Add the dependentObjectName column
result_df = joined_df.select(
    tp_df["notebookName"], 
    tp_df["sourceLocationName"], 
    tp_df["objectName"], 
    tp_df["loadType"], 
    tp_df["destinationObjectPattern"], 
    concat_ws("_", td_df["notebookName"], td_df["destinationObjectPattern"]).alias("dependentObjectName")
)

# Group by notebookName and destinationObjectPattern, then collect dependent names into a list
groupedDF = result_df.groupby("notebookName", "destinationObjectPattern", "sourceLocationName", "objectName") \
                     .agg(collect_list("dependentObjectName").alias("dependents"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch notebook names and run them
# notebooksDF = spark.sql(sqlStm)
# groupedDF = notebooksDF.groupby('notebookName','destinationObjectPattern').agg(collect_list('dependentObjectName').alias("dependents"))
activities = [
    {
        "name": row['notebookName']+'_'+row['destinationObjectPattern'],
        "path": row['notebookName'],
        "timeoutPerCellInSeconds": 90,
        "args": {"modelName":modelName},
        "dependencies": [] if row['dependents'] == [""] else row['dependents']
    } for row in groupedDF.collect()
]

DAG = {
    "activities": activities,
    "timeoutInSeconds": 43200,  # max timeout for the entire DAG
    "concurrency": 50  # max number of notebooks to run concurrently
}

print(json.dumps(DAG, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.validateDAG(DAG)

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
