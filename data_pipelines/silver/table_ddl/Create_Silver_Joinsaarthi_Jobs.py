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
    CREATE TABLE IF NOT EXISTS jobsintel.silver.joinsaarthi_jobs
    USING DELTA
    AS
    SELECT
        get_json_object(PAYLOAD, '$.job_id') AS job_id,
        get_json_object(PAYLOAD, '$.title') AS title,
        get_json_object(PAYLOAD, '$.company_id') AS company_id,
        get_json_object(PAYLOAD, '$.company_name') AS company_name,
        get_json_object(PAYLOAD, '$.created_on') AS created_on,
        get_json_object(PAYLOAD, '$.expiry') AS expiry,
        get_json_object(PAYLOAD, '$.min_salary') AS min_salary,
        get_json_object(PAYLOAD, '$.max_salary') AS max_salary,
        get_json_object(PAYLOAD, '$.experience') AS experience,
        get_json_object(PAYLOAD, '$.shift') AS shift,
        get_json_object(PAYLOAD, '$.job_type') AS job_type,
        get_json_object(PAYLOAD, '$.location') AS location,
        current_timestamp() AS BD_CREATE_DT_TM,
        current_timestamp() AS BD_UPDATE_DT_TM
    FROM jobsintel.bronze.raw_joinsaarthi_jobs
""")
