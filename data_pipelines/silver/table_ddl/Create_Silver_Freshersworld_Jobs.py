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
    CREATE TABLE IF NOT EXISTS jobsintel.silver.freshersworld_jobs
    USING DELTA
    AS
    SELECT
        get_json_object(PAYLOAD, '$.job_id') AS job_id,
        get_json_object(PAYLOAD, '$.title') AS title,
        get_json_object(PAYLOAD, '$.job_url') AS job_url,
        get_json_object(PAYLOAD, '$.company') AS company,
        get_json_object(PAYLOAD, '$.location') AS location,
        get_json_object(PAYLOAD, '$.experience') AS experience,
        get_json_object(PAYLOAD, '$.salary') AS salary,
        get_json_object(PAYLOAD, '$.job_desc') AS job_desc,
        get_json_object(PAYLOAD, '$.skills') AS skills,
        current_timestamp() AS BD_CREATE_DT_TM,
        current_timestamp() AS BD_UPDATE_DT_TM
    FROM jobsintel.bronze.raw_freshersworld_jobs
""")
