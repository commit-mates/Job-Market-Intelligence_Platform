# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC %md
# MAGIC ## Create_Silver_Freshersworld_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 13 2026| Sahithi Gudivada  | Creating the Table : freshersworld_jobs  |

# COMMAND ----------


spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.fresherworld_jobs (
        job_id STRING COMMENT 'Unique cutshort job ID',
        title STRING COMMENT 'Job title',
        job_url STRING COMMENT 'job url',
        company STRING COMMENT 'Company',
        location STRING COMMENT 'Job location',
        experience STRING COMMENT 'experience of the candidate',
        salary STRING COMMENT 'salary of the candidate',
        job_desc STRING COMMENT 'Job description',
        skills STRING COMMENT 'skills',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")

