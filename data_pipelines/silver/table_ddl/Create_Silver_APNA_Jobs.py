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
    CREATE TABLE IF NOT EXISTS jobsintel.silver.apna_jobs
    USING DELTA
    AS
    SELECT
        get_json_object(PAYLOAD, '$.job_id') AS job_id,
        get_json_object(PAYLOAD, '$.title') AS title,
        get_json_object(PAYLOAD, '$.company_id') AS company_id,
        get_json_object(PAYLOAD, '$.company_name') AS company_name,
        get_json_object(PAYLOAD, '$.last_updated') AS last_updated,
        get_json_object(PAYLOAD, '$.expiry') AS expiry,
        get_json_object(PAYLOAD, '$.department_id') AS department_id,
        get_json_object(PAYLOAD, '$.department') AS department,
        get_json_object(PAYLOAD, '$.min_salary') AS min_salary,
        get_json_object(PAYLOAD, '$.max_salary') AS max_salary,
        get_json_object(PAYLOAD, '$.min_experience') AS min_experience,
        get_json_object(PAYLOAD, '$.max_experience') AS max_experience,
        get_json_object(PAYLOAD, '$.shift') AS shift,
        get_json_object(PAYLOAD, '$.application_url') AS application_url,
        get_json_object(PAYLOAD, '$.location') AS location,
        current_timestamp() AS BD_CREATE_DT_TM,
        current_timestamp() AS BD_UPDATE_DT_TM
    FROM jobsintel.bronze.raw_apna_jobs
""")
