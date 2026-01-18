# Databricks notebook source
# MAGIC %run /Workspace/Users/supriya.yenuganti@outlook.com/Online_commerce_databricks/global

# COMMAND ----------

# MAGIC %fs ls '/Volumes/onlinecommerce/landing/sales_volume'

# COMMAND ----------

# Paths
cdm_path = "/Volumes/onlinecommerce/landing/sales_volume/SalesOrderLine.cdm.json"
manifest_path = "/Volumes/onlinecommerce/landing/sales_volume/Sales.manifest.cdm.json"
csv_path = "dbfs:/Volumes/onlinecommerce/landing/sales_volume/SalesOrderLine/*.csv"

# Build schema
schema = read_cdm_schema(cdm_path)

# Extract CSV format
delimiter, escape, newline = extract_csv_format(
    manifest_path,
    "SalesOrderLine"
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

df.write.format("delta").mode("overwrite").saveAsTable("onlinecommerce.bronze.salesorderline")
