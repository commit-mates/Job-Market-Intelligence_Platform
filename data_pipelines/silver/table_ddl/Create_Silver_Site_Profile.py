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
# MAGIC | Nov 03 2026| Sahithi Gudivada  | Creating the Table : dim_location  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.site_profile (
        SITE_NBR BIGINT  COMMENT 'Surrogate key uniquely identifying each location record',
        CITY STRING  COMMENT "Standardized city name derived from raw job location data",
        STATE STRING  COMMENT "State in which the city is located",
        PINCODE BIGINT  COMMENT "Pincode of the location",
        LATITUDE FLOAT comment "Latitude of the  location",
        LONGITUDE FLOAT comment " Longitude of the location"
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jobsintel.silver.site_profile;