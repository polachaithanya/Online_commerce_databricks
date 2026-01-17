# Databricks notebook source
vendordf = spark.table("onlinecommerce.bronze.venditems")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dimvendordf = vendordf.filter(
    vendordf.RecordId.isNotNull()
    ).select(
        vendordf.VendId.alias("VendorId")
        ,F.trim(vendordf.VendorName).alias("VendorName")
        ,F.when(vendordf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(vendordf.LastProcessedChange_DateTime).cast('TIMESTAMP').alias("LastProcessedChange_DateTime")
        ,F.from_utc_timestamp(vendordf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime")
        ,F.trim(vendordf.Address).alias("Address")
        ,F.trim(vendordf.City).alias("City")
        ,F.trim(vendordf.State).alias("State")
        ,F.trim(vendordf.Country).alias("Country")
        ,F.trim(vendordf.ZipCode).alias("ZipCode")
        ,F.trim(vendordf.Region).alias("Region")
        ,F.from_utc_timestamp(vendordf.ValidFrom,'CST').alias("ValidFrom")
        ,F.from_utc_timestamp(vendordf.ValidTo,'CST').alias("ValidTo")
        ,vendordf.Active
        ,vendordf.RecordId.alias("VendorRecordId")
        ,F.trim(vendordf.TaxId).alias("TaxId")
        ,F.trim(vendordf.CurrencyCode).alias("CurrencyCode")
    ).withColumn("UpdatedDateTime",F.current_timestamp()
    ).withColumn("VendorHashKey",F.xxhash64("VendorRecordId")
    ).withColumn("Vendordiscount",F.when(F.col("Country") == "US",F.lit(0.01)).when(F.col("Country") == "UK",F.lit(0.006)).otherwise(F.lit(0))
    )

display(dimvendordf)

# COMMAND ----------

dimvendordf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimvendor")
