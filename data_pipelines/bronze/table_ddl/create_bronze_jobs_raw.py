# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Create_Bronze_Jobs_Raw
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | May 30th 2026| Sahithi Gudivada  | Creating the Table : jobs_raw |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS jobsintel.bronze.jobs_raw (
            message_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table",
            source STRING COMMENT "Name of the source where the data is fetched",
            payload STRING  COMMENT "The Json format of the data scraped from the websites",
            bd_create_dt_tm TIMESTAMP COMMENT "timestamp loaded",
            bd_update_dt_tm TIMESTAMP COMMENT "timestamp updated"
        )
        USING DELTA
 """)
