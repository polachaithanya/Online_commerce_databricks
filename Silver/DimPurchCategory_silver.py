# Databricks notebook source
purchaseCategoryDf= spark.table("onlinecommerce.bronze.purchcategory")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimpurchaseCategoryDf = purchaseCategoryDf.filter(purchaseCategoryDf.RecordId.isNotNull()
    ).select(
       purchaseCategoryDf.CategoryId,
       F.trim(purchaseCategoryDf.CategoryName).alias("CategoryName"),
       F.when(purchaseCategoryDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseCategoryDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(purchaseCategoryDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       purchaseCategoryDf.CategoryGroupId,
       purchaseCategoryDf.RecordId.alias("PurchCategoryRecordId"),       
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("PurchCategoryHashKey", F.xxhash64("PurchCategoryRecordId")
    )
display(dimpurchaseCategoryDf)

# COMMAND ----------

dimpurchaseCategoryDf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimpurchasecategory")
