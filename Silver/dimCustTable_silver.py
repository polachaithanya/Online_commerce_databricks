# Databricks notebook source
custtablebronzedf= spark.table("onlinecommerce.bronze.custtable")


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

custtabledfdf = custtablebronzedf.filter(custtablebronzedf.RecordId.isNotNull()
    ).select(
        custtablebronzedf.CustomerId,
        F.when(custtablebronzedf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(custtablebronzedf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(custtablebronzedf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(custtablebronzedf.CustomerName).alias("CustomerName"),
        F.trim(custtablebronzedf.Email).alias("Email"),
        F.trim(custtablebronzedf.Phone).alias("Phone"),
        F.trim(custtablebronzedf.Address).alias("Address"),
        F.trim(custtablebronzedf.City).alias("City"),
        F.trim(custtablebronzedf.State).alias("State"),
        F.trim(custtablebronzedf.Country).alias("Country"),
        F.trim(custtablebronzedf.Country).alias("ZipCode"),
        F.trim(custtablebronzedf.Region).alias("Region"),
        F.from_utc_timestamp(custtablebronzedf.SignupDate,'CST').alias("SignupDate"),
        custtablebronzedf.RecordId.alias("CustRecordId")
    ).withColumn("UpdatedDateTime", F.current_timestamp()
    ).withColumn("PartyHashKey", F.xxhash64("CustRecordId")
    )
display(custtabledfdf)

# COMMAND ----------

custtabledfdf.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimcusttable")
