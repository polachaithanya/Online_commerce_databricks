# Databricks notebook source
currencyDf= spark.table("onlinecommerce.bronze.currency")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimcurrencyDf = currencyDf.filter(currencyDf.RecordId.isNotNull()
    ).select(
       currencyDf.CurrencyId,
       F.trim(currencyDf.Code).alias("CurrencyCode"),
       F.when(currencyDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(currencyDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       F.trim(currencyDf.Country).alias("Country"),
       F.trim(currencyDf.CurrencyName).alias("CurrencyName"),
       currencyDf.RecordId.alias("CurrencyRecordId"), 
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("CurrencyHashKey", F.xxhash64("CurrencyRecordId")
    )
display(dimcurrencyDf)

# COMMAND ----------

dimcurrencyDf.write.mode("overwrite").saveAsTable("onlinecommerce.silver.dimcurrency")
