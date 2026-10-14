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
    CREATE TABLE IF NOT EXISTS jobsintel.silver.cutshort_jobs
    USING DELTA
    AS
    SELECT
        get_json_object(PAYLOAD, '$.job_id') AS job_id,
        get_json_object(PAYLOAD, '$.title') AS title,
        get_json_object(PAYLOAD, '$.job_url') AS company_id,
        get_json_object(PAYLOAD, '$.company_name') AS company_name,
        get_json_object(PAYLOAD, '$.company_url') AS company_url,
        get_json_object(PAYLOAD, '$.location') AS location,
        get_json_object(PAYLOAD, '$.experience') AS experience,
        get_json_object(PAYLOAD, '$.salary') AS salary,
        get_json_object(PAYLOAD, '$.job_desc') AS job_desc,
        get_json_object(PAYLOAD, '$.skills') AS skills,
        current_timestamp() AS BD_CREATE_DT_TM,
        current_timestamp() AS BD_UPDATE_DT_TM
    FROM jobsintel.bronze.raw_cutshort_jobs
""")
