# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Joinsaarthi_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 13 2026| Sahithi Gudivada  | Creating the Table : joinsaarthi_jobs  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.joinsaarthi_jobs (
        job_id STRING COMMENT 'Unique APNA job ID',
        title STRING COMMENT 'Job title',
        company_id STRING COMMENT 'Company ID',
        company_name STRING COMMENT 'Company name',
        created_on TIMESTAMP COMMENT 'Job creation timestamp',
        expiry TIMESTAMP COMMENT 'Job expiry timestamp',
        min_salary DECIMAL(12,2) COMMENT 'Minimum salary',
        max_salary DECIMAL(12,2) COMMENT 'Maximum salary',
        experience STRING COMMENT ' experience',
        shift STRING COMMENT 'Job shift',
        job_type STRING COMMENT 'Job type',
        location STRING COMMENT 'Job location',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

