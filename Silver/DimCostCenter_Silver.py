# Databricks notebook source
costcenterDf= spark.table("onlinecommerce.bronze.CostCenter")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimcostcenterDf = costcenterDf.filter(costcenterDf.RecordId.isNotNull()
    ).select(
        costcenterDf.CostCenterNumber,
        F.when(costcenterDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(costcenterDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(costcenterDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        costcenterDf.Vat,
        costcenterDf.RecordId.alias("CostCenterRecordId"),
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("CostCenterHashKey", F.xxhash64("CostCenterRecordId")
    )
display(dimcostcenterDf)

# COMMAND ----------

dimcostcenterDf.write.mode("overwrite").saveAsTable("onlinecommerce.silver.dimcostcenter")
