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
# Description: Criação da Dim_Date
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

keyColumns = ['dsc_date']

skName = 'sk_date'
codColumn = 'dsc_date'


#Modify this value for the calendar start year
startYear = 2022
finalYear = 2030

tablePathDest = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{gold_lakehouse_id}/Tables/framework/dim_date"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install workalendar

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from workalendar.europe import Portugal
from pyspark.sql import SparkSession

# Crie uma sessão Spark
spark = SparkSession.builder.appName("HolidaysPortugal").getOrCreate()

# Crie um objeto de calendário para Portugal
cal = Portugal()

# Inicialize uma lista para armazenar os feriados
feriados = []

# Itere pelos anos no intervalo especificado
for ano in range(startYear, finalYear + 1):
    feriados_ano = cal.holidays(ano)
    feriados.extend(feriados_ano)

# Crie um DataFrame a partir dos feriados
df = spark.createDataFrame(feriados, schema=["DATE", "DSC_FERIADO"])
feriados_df = df.select(F.date_format(df.DATE,'yyyyMMdd').alias("sk_date"),
               F.when(F.col('DSC_FERIADO') == 'Christmas Eve', F.lit(1))\
                     .when(F.col('DSC_FERIADO').isNotNull(), F.lit(1))\
                     .otherwise(F.lit(0))\
                     .alias('FLG_FERIADO')).distinct()

## Identifica Quinta-feira santa
quintaFeiraSantaDF = feriados_df.filter("DSC_FERIADO = 'Good Friday' ").withColumn('COD_QUINTA_FEIRA_SANTA', F.col('sk_date')-1).select('COD_QUINTA_FEIRA_SANTA')
# quintaFeiraSantaDF.display()
# df.display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to date
dateFrom = (str(startYear) + '-01-01')
dateTo = (str(finalYear) + '-12-29')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sequence dates into dataframe
from pyspark.sql.functions import col

UNK_Values_df = spark.createDataFrame(
    [(Dtime.date(finalYear, 12, 31),), (Dtime.date(finalYear, 12, 30),)], ["dsc_date"]
)

dateDF = (
    spark.sql(
        "SELECT sequence(to_date('{0}'), to_date('{1}'), interval 1 day) AS date".format(
            dateFrom, dateTo
        )
    )
    .select(F.explode("date").alias("dsc_date"))
    .union(UNK_Values_df)
)


tempDimDF = dateDF.select(
    (
        F.year(dateDF.dsc_date) * 10000
        + F.month(dateDF.dsc_date) * 100
        + F.dayofmonth(dateDF.dsc_date)
    ).alias("sk_date"),
    F.col("dsc_date"),
    F.expr("EXTRACT(DAYOFWEEK from {} + 5 % 7 + 1)".format("dsc_date")).alias(
        "cod_day_of_week"
    ),  # F.date_format(dateDF.dsc_date, 'EEEE').alias('DSC_DAY_OF_WEEK'),\
    F.to_csv(
        F.struct(F.col("dsc_date")), {"dateFormat": "EEEE", "locale": "pt-PT"}
    ).alias(
        "dsc_day_week"
    ),  
    F.to_csv(
        F.struct(F.col("dsc_date")), {"dateFormat": "EEEE", "locale": "en-EN"}
    ).alias(
        "dsc_day_week_en"
    ),
    # F.date_format(dateDF.dsc_date, 'E').alias('DSC_DAY_OF_WEEK_SHORT'),\
    F.to_csv(F.struct(F.col("dsc_date")), {"dateFormat": "E", "locale": "pt-PT"}).alias(
        "dsc_day_week_short"
    ),
    F.to_csv(F.struct(F.col("dsc_date")), {"dateFormat": "E", "locale": "en-EN"}).alias(
        "dsc_day_week_short_en"
    ),
    F.when(
        (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 6) | (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 7), 0)
    .otherwise(1).alias("cod_day_type"),
    F.when(
        (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 6) | (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 7), "Fim de semana")
    .otherwise("Dia de semana").alias("dsc_day_type"),
    F.when(
        (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 6) | (F.expr("EXTRACT(DAYOFWEEK from dsc_date + 5 % 7 + 1)") == 7), "Weekend")
    .otherwise("Week day").alias("dsc_day_type_en"),
    F.weekofyear(dateDF.dsc_date).alias("cod_week"),
    F.lpad(F.weekofyear(dateDF.dsc_date), 2, "0").alias("dsc_week"),
    F.date_format(dateDF.dsc_date, "MM")
    .cast("int")
    .alias("cod_month"),  # F.date_format(dateDF.dsc_date, 'MMMM').alias('DSC_MONTH'),\
    F.to_csv(
        F.struct(F.col("dsc_date")), {"dateFormat": "MMMM", "locale": "pt-PT"}
    ).alias(
        "dsc_month"
    ),  
    F.to_csv(
        F.struct(F.col("dsc_date")), {"dateFormat": "MMMM", "locale": "en-EN"}
    ).alias(
        "dsc_month_en"
    ),
    # F.date_format(dateDF.dsc_date, 'MMM').alias('DSC_MONTH_SHORT'),\
    F.initcap(
        F.to_csv(F.struct(F.col("dsc_date")), {"dateFormat": "MMM", "locale": "pt-PT"})
    ).alias("dsc_month_short"),
    F.initcap(
        F.to_csv(F.struct(F.col("dsc_date")), {"dateFormat": "MMM", "locale": "en-EN"})
    ).alias("dsc_month_short_en"),
    F.year(dateDF.dsc_date).alias("cod_year"),
    (F.year(dateDF.dsc_date) * 100 + F.month(dateDF.dsc_date)).alias(
        "cod_year_month"
    ),  # F.concat_ws(' ', F.year(dateDF.dsc_date), F.date_format(dateDF.dsc_date,'MMM')).alias('DSC_YEAR_MONTH'),\
    F.concat_ws(" ", F.year(dateDF.dsc_date), F.col("dsc_month_short")).alias(
        "dsc_year_month"
    ),
    F.concat_ws(" ", F.year(dateDF.dsc_date), F.col("dsc_month_short_en")).alias(
        "dsc_year_month_en"
    ),
    F.concat(
        F.year(dateDF["dsc_date"]),
        F.lit("-S"),
        F.when(F.month(dateDF["dsc_date"]) <= 6, 1).otherwise(2),
    ).alias("cod_semester"),
    F.when(F.month(dateDF["dsc_date"]) <= 6, 1)
    .otherwise(2)
    .alias("cod_semester_short"),
    F.concat(
        F.lit("Semestre "), F.when(F.month(dateDF["dsc_date"]) <= 6, 1).otherwise(2)
    ).alias("dsc_semester"),
    F.concat(
        F.lit("Semester "), F.when(F.month(dateDF["dsc_date"]) <= 6, 1).otherwise(2)
    ).alias("dsc_semester_en"),
    F.concat(
        F.lit("S"), F.when(F.month(dateDF["dsc_date"]) <= 6, 1).otherwise(2)
    ).alias("dsc_semester_short"),
    F.concat(
        F.lit("AF"),
        F.substring(F.year(dateDF["dsc_date"]), 3, 2),
        F.lit("-S"),
        F.when(F.month(dateDF["dsc_date"]) <= 6, 1).otherwise(2),
    ).alias("cod_semester_fiscal"),
    F.quarter(dateDF.dsc_date).alias(
        "cod_trimestre"
    ),  # F.when(F.quarter(dateDF.dsc_date) == 1,'Quarter 1')\
    # .when(F.quarter(dateDF.dsc_date) == 2,'Quarter 2')\
    # .when(F.quarter(dateDF.dsc_date) == 3,'Quarter 3')\
    # .otherwise('Quarter 4').alias('DSC_QUARTER'),\
    F.when(F.quarter(dateDF.dsc_date) == 1, "trimestre 1")
    .when(F.quarter(dateDF.dsc_date) == 2, "trimestre 2")
    .when(F.quarter(dateDF.dsc_date) == 3, "trimestre 3")
    .otherwise("trimestre 4")
    .alias("dsc_trimestre"),  # F.when(F.quarter(dateDF.dsc_date) == 1,'Q1')\
    #        .when(F.quarter(dateDF.dsc_date) == 2,'Q2')\
    #        .when(F.quarter(dateDF.dsc_date) == 3,'Q3')\
    #        .otherwise('Q4').alias('DSC_QUARTER_SHORT'),\
    F.when(F.quarter(dateDF.dsc_date) == 1, "quarter 1")
    .when(F.quarter(dateDF.dsc_date) == 2, "quarter 2")
    .when(F.quarter(dateDF.dsc_date) == 3, "quarter 3")
    .otherwise("quarter 4")
    .alias("dsc_trimestre_en"),
    F.when(F.quarter(dateDF.dsc_date) == 1, "Q1")
    .when(F.quarter(dateDF.dsc_date) == 2, "Q2")
    .when(F.quarter(dateDF.dsc_date) == 3, "Q3")
    .otherwise("Q4")
    .alias("dsc_trimestre_short"),
    F.concat_ws("", F.year(dateDF.dsc_date), F.quarter(dateDF.dsc_date)).alias(
        "cod_trimestre_year"
    ),  # F.when(F.quarter(dateDF.dsc_date) == 1,F.concat_ws(' ',F.year(dateDF.dsc_date), F.lit('Q1')))\
    # .when(F.quarter(dateDF.dsc_date) == 2,F.concat_ws(' ',F.year(dateDF.dsc_date), F.lit('Q2')))\
    # .when(F.quarter(dateDF.dsc_date) == 3,F.concat_ws(' ',F.year(dateDF.dsc_date), F.lit('Q3')))\
    # .otherwise(F.concat_ws(' ',F.year(dateDF.dsc_date), F.lit('Q4'))).alias('DSC_YEAR_QUARTER'),\
    F.when(
        F.quarter(dateDF.dsc_date) == 1,
        F.concat_ws(" ", F.year(dateDF.dsc_date), F.lit("Q1")),
    )
    .when(
        F.quarter(dateDF.dsc_date) == 2,
        F.concat_ws(" ", F.year(dateDF.dsc_date), F.lit("Q2")),
    )
    .when(
        F.quarter(dateDF.dsc_date) == 3,
        F.concat_ws(" ", F.year(dateDF.dsc_date), F.lit("Q3")),
    )
    .otherwise(F.concat_ws(" ", F.year(dateDF.dsc_date), F.lit("Q4")))
    .alias("dsc_trimestre_year"),
    F.when(
        F.date_format(F.last_day(dateDF.dsc_date), "yyyyMMdd")
        == (
            F.year(dateDF.dsc_date) * 10000
            + F.month(dateDF.dsc_date) * 100
            + F.dayofmonth(dateDF.dsc_date)
        ),
        1,
    )
    .otherwise(0)
    .alias("flg_last_day_month"),
    F.when((F.month(dateDF.dsc_date) == 12) & (dateDF.dsc_date == F.last_day(dateDF.dsc_date)), 1)
    .otherwise(0)
    .alias("flg_last_day_year"),
    F.when(F.date_format(dateDF.dsc_date, "yyyy") == 1231, 1)
    .otherwise(F.concat(F.date_format(dateDF.dsc_date, "yyyy"), F.lit("1231")))
    .alias("last_day_year"),
    F.date_format(F.last_day(dateDF.dsc_date), "yyyyMMdd").alias("last_day_month"),
    F.date_format(F.date_sub(dateDF.dsc_date,F.when(F.dayofweek(dateDF.dsc_date) == 1, 6)
               .otherwise(F.dayofweek(dateDF.dsc_date)-2)), "yyyyMMdd").alias("cod_week_monday"),
    F.date_sub(dateDF.dsc_date,F.when(F.dayofweek(dateDF.dsc_date) == 1, 6)
               .otherwise(F.dayofweek(dateDF.dsc_date)-2)).alias("dt_week_monday")
)

#tempDimDF.join(DF, tempDimDF.sk_date == DF.sk_date, "left").select(
tempDimDFAlias = tempDimDF.alias("temp")
DFAlias = feriados_df.alias("df")

dateDimDF = tempDimDFAlias.join(DFAlias, col("temp.sk_date") == col("df.sk_date"), "left").select(
    col('temp.sk_date'),
    col('temp.dsc_date'),
    col('temp.cod_day_of_week'),
    col('temp.dsc_day_week'),
    col('temp.dsc_day_week_en'),
    col('temp.dsc_day_week_short'),
    col('temp.dsc_day_week_short_en'),
    col('temp.cod_day_type'),
    col('temp.dsc_day_type'),
    col('temp.dsc_day_type_en'),
    col('temp.cod_week'),
    col('temp.dsc_week'),
    col('temp.cod_month'),
    col('temp.dsc_month'),
    col('temp.dsc_month_en'),
    col('temp.dsc_month_short'),
    col('temp.dsc_month_short_en'),
    col('temp.cod_year'),
    col('temp.cod_year_month'),
    col('temp.dsc_year_month'),
    col('temp.dsc_year_month_en'),
    col('temp.cod_semester'),
    col('temp.cod_semester_short'),
    col('temp.dsc_semester'),
    col('temp.dsc_semester_en'),
    col('temp.dsc_semester_short'),
    col('temp.cod_semester_fiscal'),
    col('temp.cod_trimestre'),
    col('temp.dsc_trimestre'),
    col('temp.dsc_trimestre_en'),
    col('temp.dsc_trimestre_short'),
    col('temp.cod_trimestre_year'),
    col('temp.dsc_trimestre_year'),
    col('temp.flg_last_day_month'),
    col('temp.flg_last_day_year'),
    col('temp.last_day_year'),
    col('temp.last_day_month'),
    col('temp.cod_week_monday'),
    col('temp.dt_week_monday')
)

# #dateDimDF.filter(
# #    (dateDimDF.sk_date == "29991230") | (dateDimDF.sk_date == "29991231")
# #).display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Technical columns**
# 
# Add technical columns to the final table

# CELL ********************

dimDF = AddDimTechnicalColumns (dateDimDF, keyColumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##Write to table/file
# 
# After transformations, insert this new data to a new storage file and fill into a new delta table

# CELL ********************

dimDF.write.format("delta").option("overwriteSchema", "true").mode(
        "overwrite"
    ).save(tablePathDest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
