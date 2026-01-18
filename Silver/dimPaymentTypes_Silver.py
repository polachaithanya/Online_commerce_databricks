# Databricks notebook source
salesorderlinedf=spark.table("onlinecommerce.bronze.salesorderline")
display(salesorderlinedf)

# COMMAND ----------

# MAGIC %md
# MAGIC - 1.Normalize the tabl(create a paymenttype dimension table)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table onlinecommerce.silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists onlinecommerce.silver.dimpaymenttypes(
# MAGIC  PaymentTypeId bigint generated always as identity(start with 1 increment by 1),
# MAGIC  PaymentTypeDesc string
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### selecting the distinct PaymentTypeDesc and loading the data to dimpaymenttypes

# COMMAND ----------

from pyspark.sql.functions import col

target_df = spark.table("onlinecommerce.silver.dimpaymenttypes")
distpaymenttypedf =salesorderlinedf.select("PaymentTypeDesc").distinct()

new_records_df = (
    distpaymenttypedf.alias("s")
    .join(
        target_df.alias("t"),
        col("s.PaymentTypeDesc") == col("t.PaymentTypeDesc"),
        "left_anti"
    )
)
display(new_records_df)


# COMMAND ----------

new_records_df.write.format("delta").mode("append").saveAsTable("onlinecommerce.silver.dimpaymenttypes")
