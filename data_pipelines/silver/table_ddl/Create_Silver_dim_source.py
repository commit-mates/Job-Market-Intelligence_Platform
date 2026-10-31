# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Dim_Source
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 29 2026| Sahithi Gudivada  | Creating the Table : dim_source  |

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.dim_source (
        SOURCE_ID INT COMMENT "Source identifier",
        SOURCE_TYPE STRING COMMENT "Name of the source",
        IS_ACTIVE BOOLEAN COMMENT "Check the status of the source",
        BD_CREATE_DT DATE COMMENT 'Date loaded',
        BD_UPDATE_DT DATE COMMENT 'Date updated'
    )
    USING DELTA
""")
