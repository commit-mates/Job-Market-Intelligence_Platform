# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_APNA_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 13 2026| Sahithi Gudivada  | Creating the Table : apna_jobs  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.apna_jobs (
        JOB_ID STRING COMMENT 'Unique APNA job ID',
        TITLE STRING COMMENT 'Job title',
        COMPANY_ID STRING COMMENT 'Company ID',
        COMPANY_NAME STRING COMMENT 'Company name',
        CREATED_ON TIMESTAMP COMMENT 'Job creation timestamp',
        LAST_UPDATED TIMESTAMP COMMENT 'Last updated timestamp',
        EXPIRY TIMESTAMP COMMENT 'Job expiry timestamp',
        DEPARTMENT_ID STRING COMMENT 'Department ID',
        DEPARTMENT STRING COMMENT 'Department name',
        MIN_SALARY DECIMAL(12,2) COMMENT 'Minimum salary',
        MAX_SALARY DECIMAL(12,2) COMMENT 'Maximum salary',
        MIN_EXPERIENCE INT COMMENT 'Minimum experience',
        MAX_EXPERIENCE INT COMMENT 'Maximum experience',
        SHIFT STRING COMMENT 'Job shift',
        APPLICATION_URL STRING COMMENT 'Job application URL',
        LOCATION STRING COMMENT 'Job location',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

