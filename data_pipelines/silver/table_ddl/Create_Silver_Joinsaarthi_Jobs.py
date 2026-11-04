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
    CREATE TABLE IF NOT EXISTS jobsintel.silver.joinsaarthi_jobs (
        JOB_ID STRING COMMENT 'Unique APNA job ID',
        TITLE STRING COMMENT 'Job title',
        COMPANY_ID STRING COMMENT 'Company ID',
        company_name STRING COMMENT 'Company name',
        CREATED_ON TIMESTAMP COMMENT 'Job creation timestamp',
        EXPIRY TIMESTAMP COMMENT 'Job expiry timestamp',
        MIN_SALARY DECIMAL(12,2) COMMENT 'Minimum salary',
        MAX_SALARY DECIMAL(12,2) COMMENT 'Maximum salary',
        EXPERIENCE STRING COMMENT ' experience',
        SHIFT STRING COMMENT 'Job shift',
        JOB_TYPE STRING COMMENT 'Job type',
        CITY STRING COMMENT 'Name of the city in which job is located',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")


# COMMAND ----------

# MAGIC   %sql
# MAGIC   select *,explode_outer(split(CITY, ',')) AS CITY 
# MAGIC   from jobsintel.silver.joinsaarthi_jobs
# MAGIC   where JOB_ID = '9fce351d-dcca-4742-b0cd-0dc49e50759c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select explode_outer(CITY) as city from jobsintel.silver.joinsaarthi_jobs
# MAGIC where JOB_ID = '9fce351d-dcca-4742-b0cd-0dc49e50759c'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE jobsintel.silver.joinsaarthi_jobs

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed_locations AS (
# MAGIC     SELECT
# MAGIC         JOB_ID,
# MAGIC         FROM_JSON(CITY, 'ARRAY<STRING>') AS city_array
# MAGIC     FROM jobsintel.silver.unstop_jobs
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     JOB_ID,
# MAGIC     ci
# MAGIC FROM parsed_locations
# MAGIC LATERAL VIEW OUTER EXPLODE(city_array) AS exploded_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed_locations AS (
# MAGIC     SELECT
# MAGIC         JOB_ID,
# MAGIC         FROM_JSON(CITY, 'ARRAY<STRING>') AS city_array
# MAGIC     FROM jobsintel.silver.joinsaarthi_jobs
# MAGIC     WHERE JOB_ID = '9fce351d-dcca-4742-b0cd-0dc49e50759c'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     JOB_ID,
# MAGIC     TRIM(exploded_city) AS CITY
# MAGIC FROM parsed_locations
# MAGIC LATERAL VIEW OUTER EXPLODE(city_array) AS exploded_city;
