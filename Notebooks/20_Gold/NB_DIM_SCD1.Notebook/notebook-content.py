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

### Input Variables ###
model = 'framework'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceId, _, silver_lakehouse_id, gold_lakehouse_id = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Input Variables ###

sourceLakehouseId = sourceLakehouseId = silver_lakehouse_id
tableSource = 'standardized_tvv2t'

# Create a hash column (unique identifier), based in list of business keyColumns
keyColumns = ['COD_DIM_TYPE_1']

skName = 'SK_DIM_TYPE_1'
# Column by which the dataset will be ordered to generate the sk
codColumn = 'COD_DIM_TYPE_1'

tempTable = 'framework_temp_dim_type_1'
tableDest = 'framework.dim_type_1'
tablePathDest = tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/dim_type_1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Read Sources ###

# Option 1: build path to table and read 
# Path for the source file
sourceFilePath=f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sourceLakehouseId}/Tables/{tableSource}"
sourceDF = D.DeltaTable.forPath(spark,sourceFilePath).toDF()
#or
#sourceDF = spark.read.format("delta").load(sourceFilePath)

# Option 2: Previously create shortcuts to source tables and directly reference the shortcuts with sparkSQL
#            Make sure the schema and table name are separated by . instead of /
# sourceDF = spark.sql(f"SELECT * FROM {tableSource}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transformations ###

tempDF = sourceDF.select(sourceDF.KVGR2.alias('COD_DIM_TYPE_1'),\
                         sourceDF.BEZEI.alias('DSC_DIM_TYPE_1'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add inferred members
unkDF = UnkValues(tempDF)
tempDimDF = tempDF.union(unkDF)

# Add technical columns
dimDF = AddDimTechnicalColumns (tempDimDF, keyColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tempView = dimDF.createOrReplaceTempView(tempTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Merge to table / File ###

# if table exists, perform merge
if D.DeltaTable.isDeltaTable(spark,tablePathDest):
  print('merge')
  MergeDims (tempTable, skName, codColumn, tablePathDest)

# if table does not exists, do first load
else:
  print('load')
  LoadDims (tempTable, skName, codColumn, tablePathDest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
