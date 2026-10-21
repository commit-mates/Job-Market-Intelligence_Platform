# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Jobcliff_Silver_Transformation
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Merge                |
# MAGIC | **Source**            | jobsintel.bronze.raw_jobcliff_jobs          |
# MAGIC | **Target**            | jobsintel.silver.jobcliff_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 14th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,Import Requests
from pyspark.sql.functions import col, get_json_object, current_timestamp
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Read the Bronze table
jobcliff_bronze_df = spark.table("jobsintel.bronze.raw_jobcliff_jobs")

# COMMAND ----------

# DBTITLE 1,Extract fields from the JSON PAYLOAD
jobcliff_silver_df = jobcliff_bronze_df.select(
        get_json_object(col("PAYLOAD"), '$.job_id').alias("JOB_ID"),
        get_json_object(col("PAYLOAD"), '$.title').alias("TITLE"),
        get_json_object(col("PAYLOAD"), '$.job_type').alias("job_type"),
        get_json_object(col("PAYLOAD"), '$.company_name').alias("COMPANY_NAME"),
        get_json_object(col("PAYLOAD"), '$.location').alias("LOCATION"),
        get_json_object(col("PAYLOAD"), '$.created_on').alias("CREATED_ON"),
        get_json_object(col("PAYLOAD"), '$.salary').alias("SALARY"),
        col("BD_CREATE_DT_TM"),
        col("BD_UPDATE_DT_TM")
)

# COMMAND ----------

# DBTITLE 1,Load into the target table
targetDF = DeltaTable.forName(spark, "jobsintel.silver.jobcliff_jobs")
(
    targetDF.alias("TARGET")
    .merge(
        jobcliff_silver_df.alias("SOURCE"),
        "TARGET.JOB_ID = SOURCE.JOB_ID"
    )
    .whenMatchedUpdateAll(
        condition="""
            SOURCE.BD_UPDATE_DT_TM > TARGET.BD_UPDATE_DT_TM AND
            SOURCE.TITLE != TARGET.TITLE OR
            SOURCE.JOB_TYPE != TARGET.JOB_TYPE OR
            SOURCE.COMPANY_NAME != TARGET.COMPANY_NAME OR
            SOURCE.LOCATION != TARGET.LOCATION OR
            SOURCE.CREATED_ON != TARGET.CREATED_ON OR
            SOURCE.SALARY != TARGET.SALARY 
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

