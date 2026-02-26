# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

#####################################################################################################################################
#
# Author:      
#
# Create date: 
#
# Description: Create and Load dim time
#
#####################################################################################################################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceId, _, _, gold_lakehouse_id = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keyColumns = ['sk_time']

skName = 'sk_time'
codColumn = 'sk_time'


tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/dim_time"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the time range
start_time = '1900-01-01 00:00'
end_time = '1900-01-01 23:59'
interval_minutes = 60

# Generate a DataFrame with the time sequence
GenerateTimeOutputDF = spark.sql("""
    SELECT EXPLODE(SEQUENCE(TO_TIMESTAMP('{}'), TO_TIMESTAMP('{}'), INTERVAL {} MINUTE)) AS time """.format(start_time, end_time, interval_minutes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the transformations
timeDimDF = GenerateTimeOutputDF.withColumnRenamed("time", "date_time") \
                                .withColumn("sk_time", F.when(F.date_format("date_time", "kkmm") == "2400", "0000")\
                                                               .otherwise(F.date_format("date_time", "kkmm")))\
                                .withColumn("dsc_time", F.when(F.date_format("date_time", "kk") == "24", "00")\
                                                            .otherwise(F.date_format("date_time", "kk")))\
                                .withColumn("dsc_minute", F.date_format("date_time", "mm")) \
                                .withColumn("dsc_time_minute_24", F.when(F.date_format("date_time", "kk:mm") == "24:00", "00:00")\
                                                               .otherwise(F.date_format("date_time", "kk:mm")))\
                                .withColumn("dsc_time_minute", F.date_format("date_time", "hh:mm")) \
                                .withColumn("dsc_am_pm", F.date_format("date_time", "a"))\
                                .drop('date_time')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##Technical columns
# 
# Add technical columns to the final table

# CELL ********************

dimDF = AddDimTechnicalColumns (timeDimDF, keyColumns)
# AvoidDataFrameEvaluationError(dimDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimDF.write.format("delta").option("overwriteSchema", "true").mode(
        "overwrite"
    ).save(tablePathDest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
