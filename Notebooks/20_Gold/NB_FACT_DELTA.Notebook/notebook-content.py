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

workspaceId, _, silver_lakehouse_id, gold_lakehouse_id = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Input Variables ###
model = "framework"
sourceLakehouseId = silver_lakehouse_id
# sourceFilePath
sourceTableVbrk = 'standardized_vbrk'
sourceTableVbrp = 'standardized_vbrp'


tempTable = 'framework_temp_fact_delta'
tableDest = 'framework.fact_delta'
tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/fact_delta"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Read Sources ###

# Option 1: build path to table and read 
# Path for the source file
sourceVbrkFilePath=f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sourceLakehouseId}/Tables/{sourceTableVbrk}"
sourceVbrkDF = D.DeltaTable.forPath(spark,sourceFilePath).toDF()
sourceVbrpFilePath=f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sourceLakehouseId}/Tables/{sourceTableVbrp}"
sourceVbrpDF = D.DeltaTable.forPath(spark,sourceFilePath).toDF()
#or
#sourceDF = spark.read.format("delta").load(sourceFilePath)

# Option 2: Previously create shortcuts to source tables and directly reference the shortcuts with sparkSQL
#            Make sure the schema and table name are separated by . instead of /
# sourceDF = spark.sql(f"SELECT * FROM {tableSource}")

dimType1DF = spark.table('persistent.framework_dim_type_1')
dimDateDF = spark.table('persistent.framework_dim_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sourceVbrkFilteredDF = ProcessDelta(modelName = model, tableName = tableDest, DF = sourceVbrkDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transformations ###


joinDF = sourceVbrkDF.join(sourceVbrpDF, (sourceVbrkDF.VBELN == sourceVbrpDF.VBELN) &\
                                         (sourceVbrkDF.BI_PARTITION_MONTH == sourceVbrpDF.BI_PARTITION_MONTH) , 'left')\
                     .select(sourceVbrpDF.KVGR2,\
                             sourceVbrpDF.ERDAT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Get Dims SK ###

tempDF = joinDF.join(dimType1DF, joinDF.KVGR2 == dimType1DF.COD_DIM_TYPE_1, 'left')\
               .join(dimDateDF, joinDF.ERDAT == dimDateDF.SK_DATE, 'left')\
               .select(dimType1DF.SK_DIM_TYPE_1,\
                       dimDateDF.SK_DATE,\
                       dimDateDF.COD_YEAR_MONTH.alias('partition_key'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Replace SK null  ###

tempFactDF = ReplaceFactSkNull(tempDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Add technical columns ###

factDF = AddFactTechnicalColumns(tempFactDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Write to table / File ###

InsertOverwriteFactTables(factDF               = factDF,\
                          tablePath            = tablePathDest,\
                          overwriteSchema      = True,\
                          partitionColumnsList = ['partition_key'],\
                          partitionValue       = None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = str(Dtime.date.today())

status = UpdateProcessingDate(modelName = model, tableName = tableDest, processingDate=f'{today}')

if (status!="Completed"):
    raise("Failed to update processing date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
