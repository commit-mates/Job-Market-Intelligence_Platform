# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Cutshort_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 13 2026| Sahithi Gudivada  | Creating the Table : cutshort_jobs  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.cutshort_jobs (
        JOB_ID STRING COMMENT 'Unique cutshort job ID',
        TITLE STRING COMMENT 'Job title',
        COMPANY_NAME STRING COMMENT 'Company name',
        COMPANY_URL STRING COMMENT 'Company  URL',
        JOB_URL STRING COMMENT 'job  URL',
        LOCATION STRING COMMENT 'Job location',
        EXPERIENCE STRING COMMENT 'experience of the candidate',
        SALARY STRING COMMENT 'salary of the candidate',
        JOB_DESC STRING COMMENT 'Job description',
        SKILLS STRING COMMENT 'skills',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

