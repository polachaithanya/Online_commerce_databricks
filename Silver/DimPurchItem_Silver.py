# Databricks notebook source
purchaseItemDf = spark.table("onlinecommerce.bronze.purchitem")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimpurchaseItemDf = purchaseItemDf.filter(purchaseItemDf.RecordId.isNotNull()
    ).select(
       purchaseItemDf.ItemId,
       F.trim(purchaseItemDf.Txt).alias("ProductName"),
       F.when(purchaseItemDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseItemDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(purchaseItemDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       F.from_utc_timestamp(purchaseItemDf.ValidFrom,'CST').alias("ValidFrom"),
       F.from_utc_timestamp(purchaseItemDf.ValidTo,'CST').alias("ValidTo"), 
       purchaseItemDf.Price.alias("ProductPerUnitCost"),
       purchaseItemDf.RecordId.alias("PurchItemRecordId"),
       purchaseItemDf.CategoryID.alias("CategoryID"),       
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("PurchItemHashKey", F.xxhash64("PurchItemRecordId")
    )
display(dimpurchaseItemDf)

# COMMAND ----------

dimpurchaseItemDf.write.mode("overwrite").saveAsTable("onlinecommerce.silver.dimpurchitem")
