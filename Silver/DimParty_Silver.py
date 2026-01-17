# Databricks notebook source
partydf = spark.table("onlinecommerce.bronze.parties")
partyaddressdf=spark.table("onlinecommerce.bronze.partyaddress")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/fact tables

# COMMAND ----------

# MAGIC %md
# MAGIC - 1.Join 2 table party and party address
# MAGIC - 2.trim all string values
# MAGIC - 3.convert few timestamp column to CST
# MAGIC - 4.if any timestamp fields having null then assign the default value--> after conversion datatype will move to string
# MAGIC - 5.Added currenttimestamp to know when the data is inserted
# MAGIC - 6.Include a hash key column in each table(identify a unique row identified mostly it will be recordedId field and every table has it)

# COMMAND ----------

# DBTITLE 1,Untitled
import pyspark.sql.functions as F
p = partydf.alias("o")
pa= partyaddressdf.alias("i")
dimpartyDF= p.join(
    pa,p.PartyId == pa.PartyNumber,"left"
    ).filter(
        p.RecordId.isNotNull()
    ).select(
    p.PartyId,
    F.trim(p.PartyName).alias("PartyName"),
    F.when(p.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(p.LastProcessedChange_DateTime).cast('TIMESTAMP').alias("LastProcessedChange_DateTime")
    ,F.from_utc_timestamp(p.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime")
    ,F.trim(p.PartyAddressCode).alias("PartyAddressCode")
    ,F.from_utc_timestamp(p.EstablishedDate,'CST').alias("EstablishedDate")
    ,F.trim(p.PartyEmailId).alias("PartyEmailId")
    ,F.trim(p.PartyContactNumber).alias("PartyContactNumber")
    ,p.RecordId.alias("PartyRecordId")        
    ,F.trim(p.TaxId).alias("TaxId")
    ,F.trim(pa.Address).alias("Address")
    ,F.trim(pa.City).alias("City")
    ,F.trim(pa.State).alias("State")
    ,F.trim(pa.Country).alias("Country")
    ,F.trim(pa.Region).alias("Region")
    ,F.from_utc_timestamp(pa.ValidFrom,'CST').alias("ValidFrom")
    ,F.when(pa.ValidTo.isNull(),"1900-01-01").otherwise(pa.ValidTo).cast('TIMESTAMP').alias("ValidTo")
    ,pa.RecordId.alias("PartyAddressRecordId")
    ).withColumn("UpdatedDateTime",F.current_timestamp()
    ).withColumn("PartyHashKey",F.xxhash64("PartyRecordId")
    )
display(dimpartyDF)

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# w = Window.partitionBy("PartyId").orderBy("PartyId")
# dimrownumber =  dimpartyDF.withColumn("rn",row_number().over(w)).select("*")
# display(dimrownumber)

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing the data to silver layer

# COMMAND ----------

dimpartyDF.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.silver.dimparty")
