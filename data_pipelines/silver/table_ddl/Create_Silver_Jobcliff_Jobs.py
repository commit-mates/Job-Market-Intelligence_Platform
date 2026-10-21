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
        JOB_ID STRING COMMENT 'Unique cutshort job ID',
        TITLE STRING COMMENT 'Job title',
        JOB_TYPE STRING COMMENT 'job type',
        COMPANY_NAME STRING COMMENT 'Company name',
        LOCATION STRING COMMENT 'Job location',
        CREATED_ON STRING COMMENT 'created on',
        SALARY STRING COMMENT 'salary of the candidate',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

