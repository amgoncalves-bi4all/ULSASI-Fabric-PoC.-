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

# CELL ********************

workspaceId, _, silver_lakehouse_id, gold_lakehouse_id = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Input Variables ###

# sourceFilePath
sourceLakehouseId = silver_lakehouse_id
sourceTable = 'Sales/SalesOrderDetail_Silver'

tempTable = 'framework_temp_fact_sales_full'
tableDest = 'framework.fact_sales_full'
tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/fact_sales_full"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Read Sources ###

# Option 1: build path to table and read 
# Path for the source file
sourceSalesFilePath=f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sourceLakehouseId}/Tables/{sourceTable}"
sourceSalesDF = D.DeltaTable.forPath(spark,sourceSalesFilePath).toDF()
#or
#sourceDF = spark.read.format("delta").load(sourceFilePath)

# Option 2: Previously create shortcuts to source tables and directly reference the shortcuts with sparkSQL
#            Make sure the schema and table name are separated by . instead of /
# sourceDF = spark.sql(f"SELECT * FROM {tableSource}")

# dimType1DF = spark.table('persistent.framework_dim_type_1')
# dimDateDF = spark.table('persistent.framework_dim_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transformations ###


joinDF = sourceSalesDF\
                        .select(sourceSalesDF.SalesOrderID.alias("sk_sales_order"),\
                             sourceSalesDF.CarrierTrackingNumber.alias("cod_carrier_tracking"),\
                             sourceSalesDF.ProductID.alias("sk_product"),\
                             sourceSalesDF.OrderQty.alias("qty_ordered"),\
                             sourceSalesDF.UnitPrice.alias("val_unit_price"),\
                             sourceSalesDF.UnitPriceDiscount.alias("val_unit_price_discount"),\
                             sourceSalesDF.LineTotal.alias("val_total"),\
                             F.to_date(sourceSalesDF.ModifiedDate).alias("partition_key"),\
                             F.date_format(sourceSalesDF.ModifiedDate,"yyyyMMdd").cast(IntegerType()).alias("sk_date")\
                             )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dimDF = spark.table("framework.dim_store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Get Dims SK ###

tempDF = joinDF

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
