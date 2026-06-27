# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Create_catalog_jobsintel
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | May 30 2026| Sahithi Gudivada | Creating the Catalog : jobsintel |

# COMMAND ----------

spark.sql("""CREATE CATALOG IF NOT EXISTS jobsintel""")
