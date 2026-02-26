# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Add technical columns (based in all columns from sourceDF and based on the LoadType )
def AddMetadataColumnsStandardizedLayer (sourceDF, loadType, KeyColumnList = None):
    destDF = sourceDF
    
    if loadType == 'overwrite':
        destDF = sourceDF\
             .withColumn("CTR_FILE_NAME",F.col('_metadata').getItem('file_name'))\
             .withColumn('CTR_INS_DATE', F.current_timestamp())
    if loadType == 'append':
        destDF = sourceDF\
             .withColumn("CTR_FILE_NAME",F.col('_metadata').getItem('file_name'))\
             .withColumn('CTR_INS_DATE', F.current_timestamp())\
             .withColumn("CTR_PARTITION_KEY",F.col('_metadata').getItem('file_name').substr(-21,8))
    if loadType == 'merge':
        destDF = sourceDF\
             .withColumn("CTR_FILE_NAME",F.col('_metadata').getItem('file_name'))\
             .withColumn('CTR_INS_DATE', F.current_timestamp())\
             .withColumn('CTR_LAST_OPERATION_DATE', F.current_timestamp())\
             .withColumn('CTR_ACTION',F.lit('insert'))\
             .withColumn('CTR_IS_DELETED',F.lit(0))\
             .withColumn("CTR_HASH_KEY", F.sha2(F.concat_ws('|', *KeyColumnList ), 256))\
             .withColumn('CTR_HASH_VALIDATE', F.sha2(F.concat_ws('|', *sourceDF.columns), 256)).na.fill({'CTR_HASH_VALIDATE': 'null from stage'})
             
    return destDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def OverwriteFunction (sourceDF, destFilePath, partitionColumns):
    
    if partitionColumns == '' or  partitionColumns == None:
        (sourceDF.write.format('delta')
                       .mode('overwrite')
                       .option('overwriteSchema', 'true')
                       .save(destFilePath)
        )
         
    else:
        partitionColumnList = partitionColumns.split(',')
        
        (sourceDF.write.format('delta')
                    .mode('overwrite')
                    .partitionBy(*partitionColumnList)
                    .option('overwriteSchema', 'true')
                    .save(destFilePath)
        )
    
    # history = deltaTable.history(1).select("operationMetrics")
    # operationMetrics = history.collect()[0]["operationMetrics"]
    # numInserted = operationMetrics["numTargetRowsInserted"]
    # numUpdated = operationMetrics["numTargetRowsUpdated"]

    # result = "numInserted="+str(numInserted)+  "|numUpdated="+str(numUpdated)

    # return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def OverwriteReplaceWhereFunction (sourceDF, destFilePath, partitionColumns):
    
    snapshotColumn = 'CTR_PARTITION_KEY'
    
    if partitionColumns == '' or partitionColumns == None: 

        ### Values To Replace ###
        partitionValuesDF = sourceDF.select(snapshotColumn).distinct()
        partitionValuesList = partitionValuesDF.rdd.flatMap(lambda x: x).collect()
        partitionValues = (", ".join(repr(e) for e in partitionValuesList))
        
        #Save microbatch to delta table in a folder partitioned by Filedate 
        (sourceDF.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(snapshotColumn)
                 .option("mergeSchema", "true")
                 .option("replaceWhere",'{0} IN ({1})'.format(snapshotColumn, partitionValues))
                 .save(destFilePath)
        )

    else:
        partitionColumns = partitionColumns + " , " + snapshotColumn
        partitionColumnList = partitionColumns.replace(' ','').split(',')
        
        ### Values To Replace ###
        partitionValuesDF = sourceDF.select(snapshotColumn).distinct()
        partitionValuesList = partitionValuesDF.rdd.flatMap(lambda x: x).collect()
        partitionValues = (", ".join(repr(e) for e in partitionValuesList))
        
        #Save microbatch to delta table in a folder partitioned by Filedate 
        (sourceDF.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*partitionColumnList)
                 .option("mergeSchema", "true")
                 .option("replaceWhere",'{0} IN ({1})'.format(snapshotColumn, partitionValues))
                 .save(destFilePath)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def AppendFunction (sourceDF, destFilePath, partitionColumns):
    
    snapshotColumn = 'CTR_PARTITION_KEY'
    
    if partitionColumns == '' or partitionColumns == None: 

        #Save microbatch to delta table in a folder partitioned by Filedate 
        (sourceDF.write
                 .format("delta")
                 .mode("append")
                 .partitionBy(snapshotColumn)
                 .option("mergeSchema", "true")
                 .save(destFilePath)
        )

    else:
        partitionColumns = partitionColumns + " , " + snapshotColumn
        partitionColumnList = partitionColumns.replace(' ','').split(',')

        #Save microbatch to delta table in a folder partitioned by Filedate 
        (sourceDF.write
                 .format("delta")
                 .mode("append")
                 .partitionBy(*partitionColumnList)
                 .option("mergeSchema", "true")
                 .save(destFilePath)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def UpsertFunctionFull (destFilePath, sourceDF, KeyColumnList):
    
    deltaTable = D.DeltaTable.forPath(spark, destFilePath)

    # Function to upsert microBatchDF into Delta table using merge
    (deltaTable.alias("deltaTable").merge(sourceDF.alias("sourceDF"), "deltaTable.CTR_HASH_KEY = sourceDF.CTR_HASH_KEY")
                                   .whenMatchedUpdate(condition = ("deltaTable.CTR_HASH_VALIDATE <> sourceDF.CTR_HASH_VALIDATE"),set = {**{"CTR_ACTION" : "'update'"},**{str('deltaTable.') + lists: str('sourceDF.') + lists for lists in deltaTable.toDF().columns if lists in sourceDF.drop('CTR_ACTION', 'CTR_HASH_KEY', 'CTR_INS_DATE', *KeyColumnList).columns}})
                                   .whenNotMatchedInsertAll()
                                   .whenNotMatchedBySourceUpdate(set = {"deltaTable.CTR_IS_DELETED": "1","CTR_ACTION": "'delete'","CTR_LAST_OPERATION_DATE":F.current_timestamp()})
                                   .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def UpsertFunctionDelta (destFilePath, sourceDF, KeyColumnList):
    
    deltaTable = D.DeltaTable.forPath(spark, destFilePath)
  
    # Merge dataframe new_records_df with the existing delta table 
    (deltaTable.alias('deltaTable')
              .merge(sourceDF.alias('sourceDF'), "deltaTable.CTR_HASH_KEY = sourceDF.CTR_HASH_KEY")
              .whenMatchedUpdate(condition = ("deltaTable.CTR_HASH_VALIDATE <> sourceDF.CTR_HASH_VALIDATE"),set = {**{"CTR_ACTION" : "'update'"},**{str('deltaTable.') + lists: str('sourceDF.') + lists for lists in deltaTable.toDF().columns if lists in sourceDF.drop('CTR_ACTION', 'CTR_HASH_KEY', 'CTR_INS_DATE', *KeyColumnList).columns}})
              .whenNotMatchedInsertAll()
              .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Load Delta table - Standardized ###

# Load dataframe in full mode into path : overwrite mode

def FullLoad(sourceDF, destFilePath, partitionColumns):
        
    OverwriteFunction (sourceDF, destFilePath, partitionColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load dataframe in snapshot mode into path : append mode

def SnapshotLoad(sourceDF, destFilePath, partitionColumns, extractType):
    
    if extractType == 'full':
    
        OverwriteReplaceWhereFunction (sourceDF, destFilePath, partitionColumns)
            
    if extractType == 'delta':
        
        AppendFunction (sourceDF, destFilePath, partitionColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load dataframe in historical mode into path : upsert

def UpsertLoad(sourceDF, destFilePath, partitionColumns, extractType, KeyColumnList):

    # Case 1: table doesn't exist → full overwrite to create it
    if not D.DeltaTable.isDeltaTable(spark, destFilePath):
        OverwriteFunction(sourceDF, destFilePath, partitionColumns)
        return

    hasPartitions = partitionColumns and partitionColumns.strip() != ''

    # Case 2: partition columns defined → atomically replace only the affected partitions.
    # More efficient than a row-level merge: Delta uses partition pruning so only the
    # partitions present in sourceDF are rewritten; the rest of the table is untouched.
    if hasPartitions:
        partitionColumnList = [c.strip() for c in partitionColumns.split(',')]

        # Collect the distinct partition values present in the source batch
        partitionValuesList = sourceDF.select(*partitionColumnList).distinct().collect()

        # Build a replaceWhere predicate that covers every (multi-column) partition combination
        conditions = " AND ".join([
            "{0} IN ({1})".format(
                c,
                ", ".join(repr(row[c]) for row in partitionValuesList)
            )
            for c in partitionColumnList
        ])

        (sourceDF.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*partitionColumnList)
                 .option("replaceWhere", conditions)
                 .option("mergeSchema", "true")
                 .save(destFilePath)
        )
        return

    # Case 3: no partition columns → fall back to row-level merge
    if extractType == 'full':
        UpsertFunctionFull(destFilePath, sourceDF, KeyColumnList)

    elif extractType == 'delta':
        UpsertFunctionDelta(destFilePath, sourceDF, KeyColumnList)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
