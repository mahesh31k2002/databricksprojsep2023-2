# Databricks notebook source
print("helloo")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
 }

# COMMAND ----------


 dbutils.fs.mount(
        source = "abfss://bronze@adlsgen2dataengproject.dfs.core.windows.net/",
        mount_point = "/mnt/bronze",
        extra_configs = configs
     )

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT")

# COMMAND ----------

 dbutils.fs.mount(
        source = "abfss://silver@adlsgen2dataengproject.dfs.core.windows.net/",
        mount_point = "/mnt/silver",
        extra_configs = configs
     )

# COMMAND ----------

 dbutils.fs.mount(
        source = "abfss://gold@adlsgen2dataengproject.dfs.core.windows.net/",
        mount_point = "/mnt/gold",
        extra_configs = configs
     )

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

dbutils.fs.ls("/mnt/gold")

# COMMAND ----------


