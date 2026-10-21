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
        JOB_ID STRING COMMENT 'Unique APNA job ID',
        TITLE STRING COMMENT 'Job title',
        COMPANY_ID STRING COMMENT 'Company ID',
        company_name STRING COMMENT 'Company name',
        CREATED_ON TIMESTAMP COMMENT 'Job creation timestamp',
        EXPIRY TIMESTAMP COMMENT 'Job expiry timestamp',
        MIN_SALARY DECIMAL(12,2) COMMENT 'Minimum salary',
        MAX_SALARY DECIMAL(12,2) COMMENT 'Maximum salary',
        EXPERIENCE STRING COMMENT ' experience',
        SHIFT STRING COMMENT 'Job shift',
        JOB_TYPE STRING COMMENT 'Job type',
        LOCATION STRING COMMENT 'Job location',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

