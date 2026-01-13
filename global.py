# Databricks notebook source
import json
from pyspark.sql.types import *
def read_cdm_schema(cdm_path: str) -> StructType:
    type_mapping = {
        "String": StringType(),
        "Int64": LongType(),
        "DateTime": TimestampType()
    }

    cdm_text = spark.read.text(cdm_path)
    cdm_json = json.loads("".join(row.value for row in cdm_text.collect()))

    attributes = cdm_json["definitions"][0]["hasAttributes"]

    schema = StructType([
        StructField(
            attr["name"],
            type_mapping.get(attr["dataFormat"], StringType()),
            True
        )
        for attr in attributes
    ])

    return schema


# COMMAND ----------

import json

def extract_csv_format(
    manifest_path: str,
    entity_name: str
):
    """
    Extract delimiter, escape, and newline values from a CDM manifest
    for a given entity.

    Args:
        manifest_path (str): Path to Purchase.manifest.cdm.json
        entity_name (str): Entity name (e.g. 'PurchaseOrder')

    Returns:
        delimiter (str), escape (str), newline (str)
    """

    manifest_text = spark.read.text(manifest_path)
    manifest_json = json.loads("".join(r.value for r in manifest_text.collect()))

    entity = next(
        e for e in manifest_json["entities"]
        if e["entityName"] == entity_name
    )

    partition = entity["dataPartitionPatterns"][0]
    traits = partition["exhibitsTraits"][0]["arguments"]

    csv_options = {arg["name"]: arg["value"] for arg in traits}

    delimiter = csv_options.get("delimiter", ",")
    escape = csv_options.get("escape", "\"")
    newline = csv_options.get("newline", "\n")

    return delimiter, escape, newline


# COMMAND ----------

def read_cdm_csv(
    csv_path: str,
    schema: StructType,
    delimiter: str,
    escape: str,
    newline: str
):
    """
    Reads a headerless CSV using CDM schema and CSV format options.

    Args:
        csv_path (str): Path to CSV files (can include wildcard)
        schema (StructType): Spark schema from CDM
        delimiter (str): CSV delimiter
        escape (str): CSV escape character
        newline (str): Line separator

    Returns:
        DataFrame
    """

    df = (
        spark.read
             .option("header", "false")
             .option("delimiter", delimiter)
             .option("escape", escape)
             .option("lineSep", newline)
             .schema(schema)
             .csv(csv_path)
    )

    return df

