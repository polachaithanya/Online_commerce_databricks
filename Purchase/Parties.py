# Databricks notebook source
# MAGIC %run /Workspace/Users/supriya.yenuganti@outlook.com/Online_commerce_databricks/global

# COMMAND ----------

# Paths
cdm_path = "/Volumes/onlinecommerce/landing/purchase_volume/Parties.cdm.json"
manifest_path = "/Volumes/onlinecommerce/landing/purchase_volume/Purchase.manifest.cdm.json"
csv_path = "/Volumes/onlinecommerce/landing/purchase_volume/Parties/*.csv"

# Build schema
schema = read_cdm_schema(cdm_path)

# Extract CSV format
delimiter, escape, newline = extract_csv_format(
    manifest_path,
    "Parties"
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

df.withColumn("injested_timestamp",current_timestamp()).toTable("onlinecommerce.bronze.Parties")
