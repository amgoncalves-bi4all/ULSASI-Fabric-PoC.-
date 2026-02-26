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
model = 'Framework'

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

# sourceFilePath
sourceLakehouseId = silver_lakehouse_id
tableSource = 'standardized_tvv2t'

# Create a hash column (unique identifier), based in list of business keyColumns
keyColumns = ['cod_dim_type_2']

skName = 'sk_dim_type_2'
# Column by which the dataset will be ordered to generate the sk
codColumn = 'cod_dim_type_2'

tempTable = 'framework_temp_dim_type_2'
tableDest = 'framework.dim_type_2'
tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/dim_type_2"

# A set of columns can be defined to be considered a type 2 change. If empty all columns will be considered type 2
# If list is not empty the missing columns in the list will be type 1
historicalType2Columns = ['cod_xxxxx','atr_xxxx','atr_yyyy','cod_yyyy']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get date to process
minDateToProcess = (

    spark.read.format("delta")
        .load(
            f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/config/processDatesConfig"
        )
    .filter(
        (F.col("model") == model)
        & (F.col("tableName") == tableDest)
        & (F.col("scope") == "DimType2")
    )
    .select("date")
    .rdd.flatMap(lambda x: x)
    .collect()[0]
)

print("Temp " + str(minDateToProcess))
minDateToProcessInt = int(minDateToProcess.strftime("%Y%m%d"))

# if gold table exists, check Delta state
if D.DeltaTable.isDeltaTable(spark,tablePathDest):
    # Get info from gold table
    persistentDimDF = D.DeltaTable.forPath(spark,tablePathDest).toDF()
    # Get min CTR_INSERT_DATE from persistent table
    maxDateHistoryPersistent = (
        persistentDimDF.agg({"ctr_start_date": "max"})
        .select("max(ctr_start_date)")
        .collect()[0][0]
    )
    print("Data in persistent from " + str(maxDateHistoryPersistent))

    if minDateToProcessInt <= maxDateHistoryPersistent:
        notebookutils.notebook.exit("Date already processed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # In case source data comes from parquet files

# CELL ********************

### SourceFiles ###

### Source Info ###
sourceSystem ="bi4talent_sap_system"
objectName = "sap.kna1"

# in case of multiple source files, guarantee that files are read by order
sourceFile = GetSourcePathFileByRangeDates(sourceSystem, objectName, minDateToProcess, '29991231', "D")
sourceFilePath = ListAppend(list = sourceFile, str = mountPointRaw)
print(sourceFilePath)

if (len(sourceFilePath)==0):
  print("No Work to be done")
  dbutils.notebook.exit("No extraction done since last time")
else:
    print("New extractions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

endDate = '2999-12-31'

### Read Sources ###

sourceDF = spark.read.parquet(*sourceFilePath).withColumn("Filename",F.input_file_name())

# sourceDF.display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # In case source data comes from silver tables

# CELL ********************

endDate = '2999-12-31'

sourceFilePath=f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sourceLakehouseId}/Tables/{tableSource}"
sourceDF = D.DeltaTable.forPath(spark,sourceFilePath).toDF().filter(f"date >= '{str(minDateToProcess)}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transformations ###

def dimTransformations(sourceDF):
  tempDF = sourceDF.select(sourceDF.KUNNR.alias('cod_dim_type_2'),\
                           sourceDF.MANDT.alias('dsc_dim_type_2'),\
                           sourceDF.Filename)\
                   .distinct()
  
  return tempDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sourceDF = dimTransformations(sourceDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add inferred members 
unkDF = UnkValues(sourceDF)
tempDimDF = sourceDF.union(unkDF).drop('source')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add technical columns
dimDF = AddTechnicalColumnsHistory(tempDimDF, keyColumns, historicalType2Columns, 'Type2')

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

MergeHistoricalDim(tempTable, tablePathDest, skName, codColumn, historicalType2Columns, 'Type2')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adjust next processing date accordingly
today = str(Dtime.date.today())

UpdateProcessingDate(modelName = model, tableName = tableDest, processingDate=f'{today}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
