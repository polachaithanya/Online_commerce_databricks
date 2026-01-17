-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS onlineCommerce_EL
URL 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricks_sc)
COMMENT 'External Location for Online Commerce'

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS onlineCommerce
MANAGED LOCATION 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/'
COMMENT 'Catalog for OnlineCommerce'

-- COMMAND ----------

  USE CATALOG onlineCommerce;
  CREATE SCHEMA IF NOT EXISTS bronze
  MANAGED LOCATION 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/operations/unityCatalog/bronze/'
  COMMENT 'Bronze layer data managed here'

-- COMMAND ----------

  USE CATALOG onlineCommerce;
  CREATE SCHEMA IF NOT EXISTS silver
  MANAGED LOCATION 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/operations/unityCatalog/silver/'
  COMMENT 'silver layer data managed here'

-- COMMAND ----------

drop catalog onlineCommerce

-- COMMAND ----------

  USE CATALOG onlineCommerce;
  CREATE SCHEMA IF NOT EXISTS landing
  MANAGED LOCATION 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/landing/tables/'
  COMMENT 'Bronze layer data managed here'

-- COMMAND ----------

drop schema onlineCommerce.bronze

-- COMMAND ----------

USE CATALOG onlineCommerce;
use schema landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS purchase_volume
LOCATION 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/landing/tables/Purchase/'

-- COMMAND ----------

DESCRIBE VOLUME onlinecommerce.landing_one.purchase_volume;


-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/onlinecommerce/landing/purchase_volume'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.auth.type .adbstoreage.dfs.core.windows.net",
-- MAGIC "SAS")
-- MAGIC spark.conf.set("fs.azure.sas.token.provider.type .adbstoreage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set("fs.azure.sas.fixed.token .adbstoreage.dfs.core.windows.net",dbutils.secrets.get(scope="test_scope",key="LONGSASTOKEN"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/landing/")

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.secrets.listScopes()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.get(
-- MAGIC   "fs.azure.account.key.adbstoreage.dfs.core.windows.net",
-- MAGIC   "NOT_SET"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.auth.type.adbstoreage.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set("fs.azure.sas.token.provider.type.adbstoreage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set("fs.azure.sas.fixed.token.adbstoreage.dfs.core.windows.net", dbutils.secrets.get(scope="test_scope", key="LONGSASTOKEN"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.adbstoreage.dfs.core.windows.net",
-- MAGIC     dbutils.secrets.get(scope="test_scope", key="LONGSASTOKEN"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://onlinecommerce@adbstoreage.dfs.core.windows.net/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating connection using OAuth

-- COMMAND ----------

-- MAGIC %python
-- MAGIC service_credential = dbutils.secrets.get(scope="test_scope",key="clientSecret")
-- MAGIC
-- MAGIC spark.conf.set("fs.azure.account.auth.type .adbstoreage.dfs.core.windows.net",
-- MAGIC "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.adbstoreage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.adbstoreage.dfs.core.windows.net",
-- MAGIC                 dbutils.secrets.get(scope="test_scope",key="appid"))
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.adbstoreage.dfs.core.windows.net", service_credential)
-- MAGIC tenid=dbutils.secrets.get(scope="test_scope",key="tenatID")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.adbstoreage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenid}/oauth2/token")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC service_credential = dbutils.secrets.get(
-- MAGIC     scope="test_scope",
-- MAGIC     key="clientSecret"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.auth.type.adbstoreage.dfs.core.windows.net",
-- MAGIC     "OAuth"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.oauth.provider.type.adbstoreage.dfs.core.windows.net",
-- MAGIC     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.oauth2.client.id.adbstoreage.dfs.core.windows.net",
-- MAGIC     dbutils.secrets.get(scope="test_scope", key="appid")
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.oauth2.client.secret.adbstoreage.dfs.core.windows.net",
-- MAGIC     service_credential
-- MAGIC )
-- MAGIC
-- MAGIC tenant_id = dbutils.secrets.get(scope="test_scope", key="tenatID")
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.oauth2.client.endpoint.adbstoreage.dfs.core.windows.net",
-- MAGIC     f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
