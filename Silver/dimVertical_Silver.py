# Databricks notebook source
# MAGIC %sql
# MAGIC select * from onlinecommerce.bronze.workertable

# COMMAND ----------

workerdf= spark.table("onlinecommerce.bronze.workertable")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS onlinecommerce.silver.dimvertical (
# MAGIC   VerticalId BIGINT GENERATED ALWAYS AS IDENTITY(start with 1 increment by 1),
# MAGIC   Vertical STRING
# MAGIC )

# COMMAND ----------

df = workerdf.select(F.expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------

verticaldf = spark.table("onlinecommerce.silver.dimvertical")
display(verticaldf)

# COMMAND ----------

newrowsdf=df.filter(F.col("Vertical").isNotNull()).exceptAll(verticaldf.select("Vertical"))
display(newrowsdf)

# COMMAND ----------

spark.sql("insert into onlinecommerce.silver.dimvertical(vertical) select  Vertical from {newrowsdf}",newrowsdf=newrowsdf)
