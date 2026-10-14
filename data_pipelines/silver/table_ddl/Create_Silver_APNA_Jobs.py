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
        job_id STRING COMMENT 'Unique APNA job ID',
        title STRING COMMENT 'Job title',
        company_id STRING COMMENT 'Company ID',
        company_name STRING COMMENT 'Company name',
        created_on TIMESTAMP COMMENT 'Job creation timestamp',
        last_updated TIMESTAMP COMMENT 'Last updated timestamp',
        expiry TIMESTAMP COMMENT 'Job expiry timestamp',
        department_id STRING COMMENT 'Department ID',
        department STRING COMMENT 'Department name',
        min_salary DECIMAL(12,2) COMMENT 'Minimum salary',
        max_salary DECIMAL(12,2) COMMENT 'Maximum salary',
        min_experience INT COMMENT 'Minimum experience',
        max_experience INT COMMENT 'Maximum experience',
        shift STRING COMMENT 'Job shift',
        application_url STRING COMMENT 'Job application URL',
        location STRING COMMENT 'Job location',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

