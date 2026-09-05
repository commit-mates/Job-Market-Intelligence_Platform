# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Create_Schema_Silver
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 20th 2026| Sahithi Gudivada  | Creating the Schema : silver |

# COMMAND ----------

spark.sql("""CREATE SCHEMA IF NOT EXISTS silver""")

# COMMAND ----------

spark.sql("""select * from jobsintel.silver.dim_companies""")
