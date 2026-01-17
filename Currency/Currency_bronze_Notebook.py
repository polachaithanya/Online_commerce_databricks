# Databricks notebook source
# MAGIC %fs ls '/Volumes/onlinecommerce/landing/others_volume'

# COMMAND ----------

# MAGIC %run /Workspace/Users/supriya.yenuganti@outlook.com/Online_commerce_databricks/global

# COMMAND ----------

# Paths
cdm_path = "/Volumes/onlinecommerce/landing/others_volume/Currency.cdm.json"
manifest_path = "/Volumes/onlinecommerce/landing/others_volume/Others.manifest.cdm.json"
csv_path = "dbfs:/Volumes/onlinecommerce/landing/others_volume/currency/*.csv"

# Build schema
schema = read_cdm_schema(cdm_path)

# Extract CSV format
delimiter, escape, newline = extract_csv_format(
    manifest_path,
    "Currency"
)

# Read CSV
df = read_cdm_csv(
    csv_path,
    schema,
    delimiter,
    escape,
    newline
)

df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df.withColumn("injested_timestamp",current_timestamp()).write.saveAsTable("onlinecommerce.bronze.currency")
