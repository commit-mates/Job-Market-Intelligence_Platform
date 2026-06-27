# Databricks notebook source
# MAGIC %md
# MAGIC ## Create_Schema_Bronze
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | May 30th 2026| Sahithi Gudivada  | Creating the Table : jobs_raw |

# COMMAND ----------

spark.sql("""CREATE SCHEMA IF NOT EXISTS bronze""")
