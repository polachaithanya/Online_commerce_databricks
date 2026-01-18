# Databricks notebook source
promotablebronzedf= spark.table("onlinecommerce.bronze.promotable")


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

promotabledf = promotablebronzedf.filter(promotablebronzedf.RecordId.isNotNull()
    ).select(
        promotablebronzedf.PromotionId,
        F.when(promotablebronzedf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(promotablebronzedf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(promotablebronzedf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(promotablebronzedf.PromotionName).alias("PromotionName"),
        F.trim(promotablebronzedf.PromoCode).alias("PromoCode"),
        F.trim(promotablebronzedf.PromoType).alias("PromoType"),
        promotablebronzedf.PromoPercentage,
        F.from_utc_timestamp(promotablebronzedf.ValidFrom,'CST').alias("ValidFrom"),
        F.from_utc_timestamp(promotablebronzedf.ValidTo,'CST').alias("ValidTo"),
        promotablebronzedf.IsActive,
        promotablebronzedf.RecordId.alias("PromoRecordId")
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("PartyHashKey", F.xxhash64("PromoRecordId")
    )
display(promotabledf)

# COMMAND ----------

promotabledf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimpromotable")
