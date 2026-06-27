# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Bronze_Jobs_Raw
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Apr 3rd 2026| Sahithi Gudivada  | Creating the Table : raw_freshersworld_jobs |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS jobsintel.bronze.raw_freshersworld_jobs (
            MESSAGE_ID BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table sourcing from freshersworld",
            PAYLOAD STRING  COMMENT "The Json format of the data called from the website",
            BD_CREATE_DT_TM TIMESTAMP COMMENT "timestamp loaded",
            BD_UPDATE_DT_TM TIMESTAMP COMMENT "timestamp updated"
        )
        USING DELTA
 """)
