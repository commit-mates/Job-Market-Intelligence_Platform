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
        JOB_ID STRING COMMENT 'Unique APNA job ID',
        TITLE STRING COMMENT 'Job title',
        COMPANY_ID STRING COMMENT 'Company ID',
        COMPANY_NAME STRING COMMENT 'Company name',
        CREATED_ON TIMESTAMP COMMENT 'Job creation timestamp',
        LAST_UPDATED TIMESTAMP COMMENT 'Last updated timestamp',
        EXPIRY TIMESTAMP COMMENT 'Job expiry timestamp',
        DEPARTMENT_ID STRING COMMENT 'Department ID',
        DEPARTMENT STRING COMMENT 'Department name',
        MIN_SALARY DECIMAL(12,2) COMMENT 'Minimum salary',
        MAX_SALARY DECIMAL(12,2) COMMENT 'Maximum salary',
        MIN_EXPERIENCE INT COMMENT 'Minimum experience',
        MAX_EXPERIENCE INT COMMENT 'Maximum experience',
        SHIFT STRING COMMENT 'Job shift',
        APPLICATION_URL STRING COMMENT 'Job application URL',
        LOCATION STRING COMMENT 'Job location',
        LOCATION_ID STRING COMMENT 'Job location_id',
        CITY STRING COMMENT 'Job city',
        STATE STRING COMMENT 'Job state',
        PINCODE STRING COMMENT 'Job pincode',
        LATITUDE FLOAT COMMENT 'Job latitude',
        LONGITUDE FLOAT COMMENT 'Job longitude',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Timestamp loaded',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Timestamp updated'
    )
    USING DELTA
""")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

apna_bronze_df = spark.table("jobsintel.bronze.raw_apna_jobs")
apna_window = Window.partitionBy(
    F.to_date("created_on")
).orderBy("city")

apna_test_df = apna_bronze_df.withColumn(
    "site_nbr",
    F.concat(
        F.date_format(F.to_date("created_on"), "yyyyMMdd"),
        F.lpad(
            F.row_number().over(apna_window).cast("string"),
            2,
            "0"
        )
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  jobsintel.silver.apna_jobs