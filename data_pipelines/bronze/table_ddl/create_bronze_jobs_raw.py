# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.sql("""
CREATE DATABASE IF NOT EXISTS jobsintel
""")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS jobsintel.bronze
""")

# COMMAND ----------



jobs_raw_table_schema = spark.sql("""
                CREATE TABLE IF NOT EXISTS jobsintel.bronze.jobs_raw (
                message_id BIGINT GENERATED ALWAYS AS IDENTITY, -- The identity of the message entering the table
                source STRING,                              -- Name of the source where the data is fetched
                payload STRING,                            -- The Json format of the data scraped from the websites
                bd_create_dt_tm TIMESTAMP,                 -- timestamp loaded
                bd_update_dt_tm TIMESTAMP                   -- timestamp updated
            )
 USING DELTA
 """)































# COMMAND ----------

spark.sql("SELECT * FROM jobsintel.bronze.jobs_raw").show()
