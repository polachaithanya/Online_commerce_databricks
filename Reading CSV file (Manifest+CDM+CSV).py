# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading CDM File

# COMMAND ----------

# MAGIC %fs ls '/Volumes/onlinecommerce/landing/purchase_volume'

# COMMAND ----------

import json 
from pyspark.sql.types import *

cdm_path = "/Volumes/onlinecommerce/landing/purchase_volume/Parties.cdm.json"

cdm_text = spark.read.text(cdm_path)
cdm_json = json.loads("".join(r.value for r in cdm_text.collect()))

entity = cdm_json["definitions"][0]
attributes = entity["hasAttributes"]


# COMMAND ----------

type_mapping = {
    "String": StringType(),
    "Int64": LongType(),
    "DateTime": TimestampType()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Schema

# COMMAND ----------

schema = StructType([
    StructField(
        attr["name"],
        type_mapping.get(attr["dataFormat"], StringType()),
        True
    )
    for attr in attributes
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Manifest File

# COMMAND ----------

manifest_path = "/Volumes/onlinecommerce/landing/purchase_volume/Purchase.manifest.cdm.json"

manifest_text = spark.read.text(manifest_path)
manifest_json = json.loads("".join(r.value for r in manifest_text.collect()))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get PurchaseOrder entity config

# COMMAND ----------

purchase_entity = next(
    e for e in manifest_json["entities"]
    if e["entityName"] == "PurchaseOrder"
)

partition = purchase_entity["dataPartitionPatterns"][0]
traits = partition["exhibitsTraits"][0]["arguments"]

csv_options = {arg["name"]: arg["value"] for arg in traits}

delimiter = csv_options.get("delimiter", ",")
escape = csv_options.get("escape", "\"")
newline = csv_options.get("newline", "\n")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSv File

# COMMAND ----------

csv_path = "dbfs:/Volumes/onlinecommerce/landing/purchase_volume/Parties/*.csv"

df = (
    spark.read
         .option("header", "false")
         .option("delimiter", delimiter)
         .option("escape", escape)
         .option("lineSep", newline)
         .schema(schema)
         .csv(csv_path)
)

df.display()

