# Databricks notebook source
# MAGIC %run /Workspace/Users/supriya.yenuganti@outlook.com/Online_commerce_databricks/global

# COMMAND ----------

# MAGIC %md
# MAGIC - if we need to read the file incrementally use below path
# MAGIC - csv_path = "/Volumes/onlinecommerce/landing/purchase_volume/PartyAddress"

# COMMAND ----------

# Paths
cdm_path = "/Volumes/onlinecommerce/landing/purchase_volume/PurchContracts.cdm.json"
manifest_path = "/Volumes/onlinecommerce/landing/purchase_volume/Purchase.manifest.cdm.json"
csv_path = "/Volumes/onlinecommerce/landing/purchase_volume/PurchContracts/*.csv"


# Build schema
schema = read_cdm_schema(cdm_path)

# Extract CSV format
delimiter, escape, newline = extract_csv_format(
    manifest_path,
    "PurchContracts"
)

# Read CSV
df = read_cdm_csv(
    csv_path,
    schema,
    delimiter,
    escape,
    newline
)

from pyspark.sql.functions import current_timestamp
df.withColumn("injested_timestamp",current_timestamp()).write.saveAsTable("onlinecommerce.bronze.purchcontracts")
