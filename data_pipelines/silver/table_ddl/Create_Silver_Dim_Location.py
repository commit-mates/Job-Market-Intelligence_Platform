# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Dim_Location
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 28 2026| Sahithi Gudivada  | Creating the Table : dim_location  |

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.dim_location (
        location_key BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key uniquely identifying each location record',
        city STRING  COMMENT "Standardized city name derived from raw job location data",
        state STRING  COMMENT "State in which the city is located",
        region STRING  COMMENT "Geographical region associated with the state",
        is_remote boolean comment "Indicates whether the location represents a remote-work location"
    )
    USING DELTA
""")
