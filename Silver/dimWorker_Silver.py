# Databricks notebook source
workerdf= spark.table("onlinecommerce.bronze.workertable")
verticaldf = spark.table("onlinecommerce.silver.dimvertical")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimworkerdf = workerdf.filter(workerdf.RecordId.isNotNull()
    ).join(
        verticaldf,workerdf.Vertical == verticaldf.Vertical,"left"
    ).select(
       workerdf.WorkerID,
       F.when(workerdf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(workerdf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(workerdf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       workerdf.SupervisorId,
       F.trim(workerdf.WorkerName).alias("WorkerName"),
       F.trim(workerdf.WorkerEmail).alias("WorkerEmail"),
       F.trim(workerdf.Phone).alias("Phone"),
       F.from_utc_timestamp(workerdf.DOJ,'CST').alias("DOJ"),
       F.from_utc_timestamp(workerdf.DOL,'CST').alias("DOL"), 
       verticaldf.VerticalId ,
       workerdf.Type,
       workerdf.PayPerAnnum,
       workerdf.Rate,
       workerdf.RecordId.alias("WorkerRecordId")  
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("WorkerHashKey", F.xxhash64("WorkerRecordId")
    )
display(dimworkerdf)

# COMMAND ----------

dimworkerdf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimworker")
