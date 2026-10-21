# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Joinsaarthi_Silver_Transformation
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Merge                |
# MAGIC | **Source**            | jobsintel.bronze.raw_joinsaarthi_jobs          |
# MAGIC | **Target**            | jobsintel.silver.joinsaarthi_jobs |
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
joinsaarthi_bronze_df = spark.table("jobsintel.bronze.raw_joinsaarthi_jobs")

# COMMAND ----------

# DBTITLE 1,Extract fields from the JSON PAYLOAD
joinsaarthi_silver_df = joinsaarthi_bronze_df.select(
            get_json_object(col("PAYLOAD"), "$.job_id").alias("JOB_ID"),
            get_json_object(col("PAYLOAD"), "$.title").alias("TITLE"),
            get_json_object(col("PAYLOAD"), "$.company_id").alias("COMPANY_ID"),
            get_json_object(col("PAYLOAD"), "$.company_name").alias("COMPANY_NAME"),
            get_json_object(col("PAYLOAD"), "$.created_on").alias("CREATED_ON"),
            get_json_object(col("PAYLOAD"), "$.expiry").alias("EXPIRY"),
            get_json_object(col("PAYLOAD"), "$.min_salary").alias("MIN_SALARY"),
            get_json_object(col("PAYLOAD"), "$.max_salary").alias("MAX_SALARY"),
            get_json_object(col("PAYLOAD"), "$.experience").alias("EXPERIENCE"),
            get_json_object(col("PAYLOAD"), "$.shift").alias("SHIFT"),
            get_json_object(col("PAYLOAD"), "$.job_type").alias("JOB_TYPE"),
            get_json_object(col("PAYLOAD"), "$.location").alias("LOCATION"),
            col("BD_CREATE_DT_TM"),
            col("BD_UPDATE_DT_TM")
)

# COMMAND ----------

# DBTITLE 1,Load into the target table
targetDF = DeltaTable.forName(spark, "jobsintel.silver.joinsaarthi_jobs")
(
    targetDF.alias("TARGET")
    .merge(
        joinsaarthi_silver_df.alias("SOURCE"),
        "TARGET.JOB_ID = SOURCE.JOB_ID"
    )
    .whenMatchedUpdateAll(
        condition="""
            SOURCE.BD_UPDATE_DT_TM > TARGET.BD_UPDATE_DT_TM AND 
            SOURCE.TITLE != TARGET.TITLE OR
            SOURCE.COMPANY_ID != TARGET.COMPANY_ID OR
            SOURCE.COMPANY_NAME != TARGET.COMPANY_NAME OR
            SOURCE.CREATED_ON != TARGET.CREATED_ON OR
            SOURCE.EXPIRY != TARGET.EXPIRY OR
            SOURCE.MIN_SALARY != TARGET.MIN_SALARY OR
            SOURCE.MAX_SALARY != TARGET.MAX_SALARY OR
            SOURCE.EXPERIENCE != TARGET.EXPERIENCE OR
            SOURCE.SHIFT != TARGET.SHIFT OR 
            SOURCE.JOB_TYPE != TARGET.JOB_TYPE OR
            SOURCE.LOCATION != TARGET.LOCATION 
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

