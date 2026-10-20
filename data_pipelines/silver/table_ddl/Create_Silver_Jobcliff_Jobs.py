# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Jobcliff_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 13 2026| Sahithi Gudivada  | Creating the Table : jobcliff_jobs  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.jobcliff_jobs (
        job_id STRING COMMENT 'Unique cutshort job ID',
        title STRING COMMENT 'Job title',
        job_type STRING COMMENT 'job type',
        company_name STRING COMMENT 'Company name',
        location STRING COMMENT 'Job location',
        created_on STRING COMMENT 'created on',
        salary STRING COMMENT 'salary of the candidate',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

