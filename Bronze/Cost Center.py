# Databricks notebook source
# MAGIC %md 
# MAGIC - this table constists of vat value

# COMMAND ----------

# MAGIC %run /Workspace/Users/supriya.yenuganti@outlook.com/Online_commerce_databricks/global

# COMMAND ----------

# MAGIC %fs ls '/Volumes/onlinecommerce/landing/others_volume'

# COMMAND ----------

# Paths
cdm_path = "/Volumes/onlinecommerce/landing/others_volume/CostCenter.cdm.json"
manifest_path = "/Volumes/onlinecommerce/landing/others_volume/Others.manifest.cdm.json"
csv_path = "dbfs:/Volumes/onlinecommerce/landing/others_volume/costcenter/*.csv"

# Build schema
schema = read_cdm_schema(cdm_path)

# Extract CSV format
#second input is the name of the entity(single to cdm file name)
delimiter, escape, newline = extract_csv_format(
    manifest_path,
    "CostCenter"
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
df.withColumn("injested_timestamp",current_timestamp()).write.saveAsTable("onlinecommerce.bronze.costcenter")
